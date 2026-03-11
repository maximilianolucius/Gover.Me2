#!/usr/bin/env python3
"""
RAG Document Uploader - Versión con Servicio Remoto + Fallback Local
Prioridad: Servidor remoto -> Fallback: CPU local
"""

import os
import sys
import json
import hashlib
import argparse
import requests
import time
import random
from typing import List, Dict, Optional
from pathlib import Path
from dotenv import load_dotenv

# Asegurar que el root del proyecto esté en sys.path para poder importar rag_config
_CURRENT_DIR = os.path.dirname(__file__)
_ROOT_DIR = os.path.abspath(os.path.join(_CURRENT_DIR, '..'))
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

from rag_config import DEFAULT_EMBEDDING_MODEL
import psycopg2
import hashlib
from typing import Dict, Optional
from datetime import datetime
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pymilvus import FieldSchema, CollectionSchema, DataType, Collection, utility, connections
from sentence_transformers import SentenceTransformer
from psycopg2.extras import execute_values

try:
    import orjson as _json_fast
except Exception:  # fallback
    _json_fast = None


# Cargar variables de entorno
load_dotenv()

DEFAULT_EMBEDDING_MODEL = DEFAULT_EMBEDDING_MODEL
EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://172.24.165.168:5000")
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "64"))
LOCAL_EMBED_BATCH_SIZE = int(os.getenv("LOCAL_EMBED_BATCH_SIZE", "32"))
MILVUS_BUFFER_SIZE = int(os.getenv("MILVUS_BUFFER_SIZE", "2000"))


class HybridEmbeddings:
    """Embeddings híbridos: Servicio remoto con fallback local"""

    def __init__(self, model_name: str = DEFAULT_EMBEDDING_MODEL, service_url: str = EMBEDDING_SERVICE_URL):
        self.model_name = model_name
        self.service_url = service_url.rstrip('/')
        self.local_model = None
        self.dimension = None
        self.use_remote = True
        self.session = requests.Session()

        print(f"🔄 Configurando embeddings híbridos...")
        print(f"   Servicio remoto: {self.service_url}")
        print(f"   Modelo: {model_name}")

        # Intentar servicio remoto primero
        if self._test_remote_service():
            print("✅ Servicio remoto disponible")
        else:
            print("⚠️  Servicio remoto no disponible, iniciando modo local")
            self._init_local_model()

    def _test_remote_service(self) -> bool:
        """Probar si el servicio remoto está disponible"""
        try:
            response = requests.get(f"{self.service_url}/health", timeout=5)
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get("model", {}).get("loaded"):
                    self.dimension = health_data["model"].get("dimension")
                    return True
        except Exception as e:
            print(f"   ❌ Error conectando al servicio: {e}")
        return False

    def _init_local_model(self):
        """Inicializar modelo local como fallback"""
        print(f"🔄 Cargando modelo local en CPU: {self.model_name}")
        self.local_model = SentenceTransformer(self.model_name, device='cpu')
        self.dimension = self.local_model.get_sentence_embedding_dimension()
        self.use_remote = False
        print(f"✅ Modelo local cargado (dim={self.dimension})")

    def _try_remote_embed(self, texts: List[str], is_batch: bool = False, batch_size: int | None = None) -> Optional[List[List[float]]]:
        """Intentar embedding remoto"""
        last_err = None
        for attempt in range(3):
            try:
                if is_batch:
                    payload = {"texts": texts}
                    if batch_size:
                        payload["batch_size"] = batch_size
                    response = self.session.post(
                        f"{self.service_url}/embed/batch",
                        json=payload,
                        timeout=60
                    )
                else:
                    payload = {"text": texts[0]}
                    response = self.session.post(
                        f"{self.service_url}/embed",
                        json=payload,
                        timeout=30
                    )

                if response.status_code == 200:
                    data = response.json()
                    if is_batch:
                        return data.get("embeddings", [])
                    else:
                        embedding = data.get("embedding", [])
                        return [embedding] if embedding else []
                else:
                    last_err = f"HTTP {response.status_code}: {response.text[:200]}"
            except Exception as e:
                last_err = str(e)

            # backoff
            time.sleep(0.5 * (2 ** attempt) + random.random() * 0.2)

        print(f"   ⚠️  Error en servicio remoto tras reintentos: {last_err}")
        return None

    def embed_query(self, text: str) -> List[float]:
        """Embedding de consulta individual"""
        # Intentar servicio remoto
        if self.use_remote:
            result = self._try_remote_embed([text], is_batch=False)
            if result and len(result) > 0:
                return result[0]

            # Si falla, cambiar a local permanentemente
            print("🔄 Cambiando a modo local...")
            if self.local_model is None:
                self._init_local_model()
            self.use_remote = False

        # Usar modelo local
        vec = self.local_model.encode(text, normalize_embeddings=True)
        return vec.tolist()

    def embed_documents(self, texts: List[str], batch_size: Optional[int] = None) -> List[List[float]]:
        """Embedding de múltiples documentos"""
        batch_size = batch_size or EMBEDDING_BATCH_SIZE
        # Intentar servicio remoto para lotes
        if self.use_remote and len(texts) > 0:
            result = self._try_remote_embed(texts, is_batch=True, batch_size=batch_size)
            if result and len(result) == len(texts):
                return result

            # Si falla, cambiar a local
            print("🔄 Cambiando a modo local para lotes...")
            if self.local_model is None:
                self._init_local_model()
            self.use_remote = False

        # Usar modelo local
        if self.local_model is None:
            self._init_local_model()

        bs = max(1, int(os.getenv("LOCAL_EMBED_BATCH_SIZE", str(LOCAL_EMBED_BATCH_SIZE))))
        vecs = self.local_model.encode(texts, normalize_embeddings=True, batch_size=bs)
        return [v.tolist() for v in vecs]

    def get_status(self) -> Dict:
        """Obtener estado actual del sistema de embeddings"""
        return {
            "mode": "remote" if self.use_remote else "local",
            "service_url": self.service_url if self.use_remote else None,
            "model_name": self.model_name,
            "dimension": self.dimension,
            "local_loaded": self.local_model is not None
        }


class DatabaseManager:
    """Gestor simplificado de PostgreSQL"""

    def __init__(self):
        self.connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "rag_chatbot"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        self._create_tables()

    def _create_tables(self):
        """Crear tablas necesarias"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id SERIAL PRIMARY KEY,
                    titulo VARCHAR(500),
                    autor VARCHAR(200),
                    fecha TIMESTAMP,
                    url_original TEXT,
                    formato_detectado VARCHAR(100),
                    contenido_completo TEXT,
                    content_hash VARCHAR(32),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS text_chunks (
                    id SERIAL PRIMARY KEY,
                    document_id INTEGER REFERENCES documents(id),
                    chunk_text TEXT,
                    chunk_index INTEGER,
                    vector_id VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Índices para performance y unicidad
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS documents_content_hash_idx ON documents(content_hash)")
            cursor.execute("CREATE INDEX IF NOT EXISTS text_chunks_document_id_idx ON text_chunks(document_id)")

            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def document_exists(self, doc_data: Dict) -> Optional[int]:
        """Conservado por compatibilidad; usa índice único y SELECT directo"""
        cursor = self.connection.cursor()
        try:
            content = doc_data.get('contenido_completo', '')
            content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
            cursor.execute("SELECT id FROM documents WHERE content_hash = %s", (content_hash,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def _truncate(self, value: Optional[str], max_len: int) -> Optional[str]:
        if value is None:
            return None
        s = str(value)
        return s[:max_len]

    def _coerce_timestamp(self, value) -> Optional[datetime]:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value
        # Aceptar formatos ISO comunes; si falla, devolver None para evitar aborto de transacción
        try:
            # fromisoformat soporta 'YYYY-MM-DD' y 'YYYY-MM-DDTHH:MM:SS' (sin Z)
            return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except Exception:
            return None

    def insert_document(self, doc_data: Dict, precomputed_hash: Optional[str] = None) -> tuple[int, bool]:
        """Insertar documento con UPSERT.
        Devuelve (doc_id, inserted) donde inserted indica si se creó una fila nueva.
        """
        cursor = self.connection.cursor()
        try:
            content = doc_data.get('contenido_completo', '')
            content_hash = precomputed_hash or hashlib.md5(content.encode('utf-8')).hexdigest()

            titulo = self._truncate(doc_data.get('titulo'), 500)
            autor = self._truncate(doc_data.get('autor'), 200)
            formato = self._truncate(doc_data.get('formato_detectado'), 100)
            fecha = self._coerce_timestamp(doc_data.get('fecha'))

            cursor.execute(
                """
                INSERT INTO documents (titulo, autor, fecha, url_original, formato_detectado, contenido_completo, content_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_hash) DO NOTHING
                RETURNING id
                """,
                (
                    titulo,
                    autor,
                    fecha,
                    doc_data.get('url_original'),
                    formato,
                    content,
                    content_hash,
                ),
            )

            row = cursor.fetchone()
            if row is None:
                # Ya existía; obtener id
                cursor.execute("SELECT id FROM documents WHERE content_hash = %s", (content_hash,))
                doc_id = cursor.fetchone()[0]
                inserted = False
            else:
                doc_id = row[0]
                inserted = True
            self.connection.commit()
            return doc_id, inserted
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def insert_chunks_bulk(self, document_id: int, rows: List[tuple]):
        """Inserción masiva de chunks en una sola transacción.
        rows: List de tuplas (document_id, chunk_text, chunk_index, vector_id)
        """
        cursor = self.connection.cursor()
        try:
            execute_values(
                cursor,
                "INSERT INTO text_chunks (document_id, chunk_text, chunk_index, vector_id) VALUES %s",
                rows,
                page_size=1000,
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()


class MilvusManager:
    """Gestor simplificado de Milvus"""

    def __init__(self, collection_name: str = "document_vectors", dim: int = 384):
        self.collection_name = collection_name
        self._connect()
        self._setup_collection(dim)

    def _connect(self):
        """Conectar a Milvus"""
        connections.connect(
            host=os.getenv("MILVUS_HOST", "localhost"),
            port=os.getenv("MILVUS_PORT", "19530")
        )

    def _setup_collection(self, dim: int):
        """Configurar colección de Milvus"""
        if utility.has_collection(self.collection_name):
            self.collection = Collection(self.collection_name)
            # Verificar dimensión del campo vector
            try:
                vec_field = next((f for f in self.collection.schema.fields if f.name == "vector"), None)
                if vec_field is not None and hasattr(vec_field, 'params'):
                    existing_dim = vec_field.params.get('dim')
                    if existing_dim and int(existing_dim) != int(dim):
                        print(
                            f"⚠️ Advertencia: la colección existente '{self.collection_name}' usa dim={existing_dim}, "
                            f"pero el modelo actual produce dim={dim}. Inserciones podrían fallar.")
            except Exception:
                pass
        else:
            fields = [
                FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=100, is_primary=True),
                FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
                FieldSchema(name="document_id", dtype=DataType.INT64),
                FieldSchema(name="chunk_index", dtype=DataType.INT64),
                FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=10000),
            ]
            schema = CollectionSchema(fields, "Vectores de documentos RAG")
            self.collection = Collection(name=self.collection_name, schema=schema)

            # Crear índice
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {"M": 16, "efConstruction": 100},
            }
            self.collection.create_index("vector", index_params)

        self.collection.load()

    def insert_vectors(self, vectors_data: List[Dict], flush: bool = True):
        """Insertar vectores en Milvus"""
        if not vectors_data:
            return

        ids = [item["id"] for item in vectors_data]
        vectors = [item["vector"] for item in vectors_data]
        document_ids = [item["document_id"] for item in vectors_data]
        chunk_indices = [item["chunk_index"] for item in vectors_data]
        texts = [item["text"] for item in vectors_data]

        data = [ids, vectors, document_ids, chunk_indices, texts]
        self.collection.insert(data)
        if flush:
            self.collection.flush()

    def flush(self):
        try:
            self.collection.flush()
        except Exception:
            pass


class DocumentUploader:
    """Clase principal para subir documentos"""

    def __init__(self):
        print("🔌 Conectando a bases de datos...")
        self.db_manager = DatabaseManager()
        # Cargar embeddings híbridos
        self.embeddings = HybridEmbeddings()
        self.milvus_manager = MilvusManager(dim=self.embeddings.dimension)
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len
        )
        # Buffer de vectores para Milvus
        self._vectors_buffer: List[Dict] = []
        self._milvus_buffer_size = MILVUS_BUFFER_SIZE

        # Mostrar estado
        status = self.embeddings.get_status()
        print(f"✅ Sistema inicializado ({status['mode']} mode)")

    @staticmethod
    def _batched(seq: List[str], size: int) -> List[List[str]]:
        for i in range(0, len(seq), size):
            yield seq[i:i + size]

    def find_json_files(self, directory: str) -> List[str]:
        """Buscar recursivamente archivos JSON (.json y .json.gz) en directorio"""
        json_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.json') or file.endswith('.json.gz'):
                    json_files.append(os.path.join(root, file))
        return json_files

    def process_document(self, doc_data: Dict) -> Optional[int]:
        """Procesar un documento y generar vectores"""
        # Calcular hash una vez
        content = doc_data.get('contenido_completo', '')
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        # Si ya existe, evitar trabajo pesado
        existing_id = self.db_manager.document_exists({"contenido_completo": content})
        if existing_id is not None:
            return None

        # Insertar documento primero (para obtener doc_id y evitar trabajo si hay carrera)
        doc_id, inserted = self.db_manager.insert_document(doc_data, precomputed_hash=content_hash)
        if not inserted:
            return None

        # Procesar contenido
        content = (content or '').strip()
        if not content:
            return doc_id

        # Dividir en chunks
        chunks = self.text_splitter.split_text(content)
        if not chunks:
            return doc_id

        # Generar vectores en sublotes
        total_chunks = len(chunks)
        print(f"   🔄 Generando {total_chunks} embeddings...", end="")
        embeddings: List[List[float]] = []
        for sub in self._batched(chunks, EMBEDDING_BATCH_SIZE):
            sub_vecs = self.embeddings.embed_documents(sub, batch_size=EMBEDDING_BATCH_SIZE)
            embeddings.extend(sub_vecs)

        if len(embeddings) != total_chunks:
            raise RuntimeError(f"Embeddings desalineados: {len(embeddings)} vs chunks {total_chunks}")

        # Construir filas y vector payloads
        rows = []
        vectors_data: List[Dict] = []
        for i, (chunk, vec) in enumerate(zip(chunks, embeddings)):
            if not chunk.strip():
                continue
            vector_id = f"{doc_id}_{i}"
            rows.append((doc_id, chunk, i, vector_id))
            vectors_data.append({
                "id": vector_id,
                "vector": vec,
                "document_id": doc_id,
                "chunk_index": i,
                "text": chunk[:9000],
            })

        # Inserción masiva de chunks en una sola transacción
        if rows:
            self.db_manager.insert_chunks_bulk(doc_id, rows)

        # Bufferizar vectores para Milvus e insertar por lotes grandes
        self._vectors_buffer.extend(vectors_data)
        if len(self._vectors_buffer) >= self._milvus_buffer_size:
            self.milvus_manager.insert_vectors(self._vectors_buffer, flush=False)
            self._vectors_buffer.clear()
        print(f" {len(vectors_data)} vectores")

        return doc_id

    def upload_from_directory(self, directory: str):
        """Subir todos los documentos JSON de un directorio recursivamente"""
        # Asegurar enlace simbólico a almacenamiento externo si aplica
        try:
            base_norm = os.path.normpath(directory)
            parts = base_norm.split(os.sep)
            if 'rag_document_data' in parts:
                idx = parts.index('rag_document_data')
                local_root = os.sep.join(parts[:idx + 1])
                if not os.path.isabs(local_root):
                    local_root = os.path.join(os.getcwd(), local_root)
                mount_point = '/mnt/disco6tb'
                target_root = '/mnt/disco6tb/Gover.Me/rag_document_data'
                if os.path.ismount(mount_point) or os.path.isdir(mount_point):
                    os.makedirs(target_root, exist_ok=True)
                    if not os.path.exists(local_root):
                        os.symlink(target_root, local_root)
        except Exception as e:
            print(f"⚠️ No se pudo asegurar enlace simbólico: {e}")

        if not os.path.exists(directory):
            print(f"❌ Directorio no encontrado: {directory}")
            return

        # Encontrar archivos JSON
        json_files = self.find_json_files(directory)
        # Mezclar el orden de procesamiento para distribuir carga entre instancias
        try:
            env_seed = os.getenv("RAG_SHUFFLE_SEED")
            if env_seed is not None:
                seed = int(env_seed)
            else:
                seed = (int.from_bytes(os.urandom(8), 'big') ^ int(time.time() * 1e6) ^ os.getpid()) & ((1 << 64) - 1)
            rnd = random.Random(seed)
            rnd.shuffle(json_files)
            print(f"🔀 Archivos mezclados (seed={seed})")
        except Exception:
            # Si algo falla, continuar sin mezclar
            pass
        if not json_files:
            print(f"⚠️ No se encontraron archivos JSON en: {directory}")
            return

        print(f"📄 Encontrados {len(json_files)} archivos JSON")

        # Procesar archivos
        loaded = 0
        skipped = 0
        errors = 0

        for i, file_path in enumerate(json_files, 1):
            parse_failed = False
            try:
                print(f"📄 [{i}/{len(json_files)}] {os.path.basename(file_path)}", end="")

                # Lectura y parseo del JSON (aislado para no borrar archivos por errores de proceso)
                try:
                    if file_path.endswith('.json.gz'):
                        import gzip as _gzip
                        with _gzip.open(file_path, 'rb') as f:
                            raw = f.read()
                            if _json_fast:
                                doc_data = _json_fast.loads(raw)
                            else:
                                doc_data = json.loads(raw.decode('utf-8'))
                    else:
                        if _json_fast:
                            with open(file_path, 'rb') as f:
                                doc_data = _json_fast.loads(f.read())
                        else:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                doc_data = json.load(f)
                except Exception:
                    parse_failed = True
                    raise

                doc_id = self.process_document(doc_data)

                if doc_id is None:
                    print(" ⭐ Ya existe")
                    skipped += 1
                else:
                    print(f" ✅ (ID: {doc_id})")
                    loaded += 1

            except Exception as e:
                print(f" ❌ Error: {str(e)}")
                # Asegurar que la conexión no quede en estado abortado para el siguiente archivo
                try:
                    self.db_manager.connection.rollback()
                except Exception:
                    pass
                # Solo borrar .json.gz si falló el parseo (archivo corrupto/truncado)
                if parse_failed and file_path.endswith('.json.gz'):
                    try:
                        os.remove(file_path)
                        print("   🗑️ Eliminado archivo .json.gz corrupto")
                    except Exception:
                        pass
                errors += 1

        # Resumen
        print(f"\n📊 Resumen:")
        print(f"   ✅ Cargados: {loaded}")
        print(f"   ⭐ Ya existían: {skipped}")
        print(f"   ❌ Errores: {errors}")

        # Estado final del sistema
        final_status = self.embeddings.get_status()
        print(f"   🔧 Modo final: {final_status['mode']}")
        # Flush final de Milvus si quedan vectores bufferizados
        if self._vectors_buffer:
            self.milvus_manager.insert_vectors(self._vectors_buffer, flush=True)
            self._vectors_buffer.clear()


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description="RAG Document Uploader - Servicio remoto con fallback local")
    parser.add_argument("directory", help="Directorio con documentos JSON")

    args = parser.parse_args()

    try:
        uploader = DocumentUploader()
        uploader.upload_from_directory(args.directory)
        print("🎉 Proceso completado")

    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"❌ Error fatal: {e}")


if __name__ == "__main__":
    main()
