"""
Módulo de configuración y gestión de ArangoDB para el sistema Nexus de análisis de turismo.
Proporciona funcionalidades para conexión, inicialización de colecciones e índices.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from pyArango.connection import Connection
from pyArango.collection import Collection, Field
from pyArango.graph import Graph, EdgeDefinition

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()


class NexusDB:
    """
    Clase para gestionar la conexión y operaciones con ArangoDB para el sistema Nexus.
    """

    def __init__(self):
        """Inicializa la conexión a ArangoDB usando variables de entorno."""
        self.host = os.getenv("ARANGO_HOST", "localhost")
        self.port = int(os.getenv("ARANGO_PORT", "8529"))
        self.user = os.getenv("ARANGO_USER", "root")
        self.password = os.getenv("ARANGO_PASSWORD", "")
        self.db_name = os.getenv("ARANGO_DB", "pdfkg")

        self.connection = None
        self.db = None

    def connect(self) -> bool:
        """
        Establece conexión con ArangoDB.

        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario
        """
        try:
            url = f"http://{self.host}:{self.port}"
            logger.info(f"Conectando a ArangoDB en {url}...")

            self.connection = Connection(
                arangoURL=url,
                username=self.user,
                password=self.password
            )

            # Verificar si la base de datos existe, si no crearla
            if not self.connection.hasDatabase(self.db_name):
                logger.info(f"Creando base de datos '{self.db_name}'...")
                self.connection.createDatabase(name=self.db_name)

            self.db = self.connection[self.db_name]
            logger.info(f"Conectado exitosamente a la base de datos '{self.db_name}'")
            return True

        except Exception as e:
            logger.error(f"Error al conectar con ArangoDB: {e}")
            return False

    def initialize_collections(self) -> bool:
        """
        Crea las colecciones necesarias para el sistema Nexus si no existen.

        Returns:
            bool: True si las colecciones fueron creadas/verificadas exitosamente
        """
        try:
            # Colección principal de métricas de turismo
            if not self.db.hasCollection("metricas_turismo"):
                logger.info("Creando colección 'metricas_turismo'...")
                self.db.createCollection(name="metricas_turismo")
                logger.info("Colección 'metricas_turismo' creada exitosamente")
            else:
                logger.info("Colección 'metricas_turismo' ya existe")

            # Colección para historial de Q&A (si no existe)
            if not self.db.hasCollection("qa_history"):
                logger.info("Creando colección 'qa_history'...")
                self.db.createCollection(name="qa_history")
                logger.info("Colección 'qa_history' creada exitosamente")
            else:
                logger.info("Colección 'qa_history' ya existe")

            return True

        except Exception as e:
            logger.error(f"Error al inicializar colecciones: {e}")
            return False

    def create_indexes(self) -> bool:
        """
        Crea índices en la colección metricas_turismo para optimizar consultas.

        Returns:
            bool: True si los índices fueron creados exitosamente
        """
        try:
            collection = self.db["metricas_turismo"]

            # Índice por categoría
            logger.info("Creando índice en campo 'categoria'...")
            collection.ensureHashIndex(["categoria"], unique=False, sparse=False)

            # Índice por año
            logger.info("Creando índice en campo 'anio'...")
            collection.ensureHashIndex(["anio"], unique=False, sparse=False)

            # Índice por mes
            logger.info("Creando índice en campo 'mes'...")
            collection.ensureHashIndex(["mes"], unique=False, sparse=False)

            # Índice por nombre de métrica
            logger.info("Creando índice en campo 'metrica_nombre'...")
            collection.ensureHashIndex(["metrica_nombre"], unique=False, sparse=False)

            # Índice compuesto por categoría + año + mes para queries frecuentes
            logger.info("Creando índice compuesto en 'categoria', 'anio', 'mes'...")
            collection.ensureHashIndex(["categoria", "anio", "mes"], unique=False, sparse=False)

            # Índice por tipo de periodo
            logger.info("Creando índice en campo 'periodo_tipo'...")
            collection.ensureHashIndex(["periodo_tipo"], unique=False, sparse=False)

            logger.info("Todos los índices creados exitosamente")
            return True

        except Exception as e:
            logger.error(f"Error al crear índices: {e}")
            return False

    def insert_metric(self, metric_data: Dict[str, Any]) -> Optional[str]:
        """
        Inserta una métrica en la colección metricas_turismo.

        Args:
            metric_data: Diccionario con los datos de la métrica

        Returns:
            str: ID del documento insertado, None si hubo error
        """
        try:
            collection = self.db["metricas_turismo"]
            doc = collection.createDocument(metric_data)
            doc.save()
            return doc._key
        except Exception as e:
            logger.error(f"Error al insertar métrica: {e}")
            return None

    def bulk_insert_metrics(self, metrics: List[Dict[str, Any]]) -> int:
        """Inserta o actualiza múltiples métricas evitando duplicados."""
        if not metrics:
            return 0

        upsert_query = """
UPSERT {
    categoria: @categoria,
    anio: @anio,
    mes: @mes,
    periodo_tipo: @periodo_tipo,
    metrica_nombre: @metrica_nombre
}
INSERT MERGE(@data, {
    created_at: DATE_ISO8601(DATE_NOW()),
    updated_at: DATE_ISO8601(DATE_NOW())
})
UPDATE MERGE(OLD, @data, {
    updated_at: DATE_ISO8601(DATE_NOW())
}) IN metricas_turismo
RETURN { is_inserted: IS_NULL(OLD), doc: NEW }
"""

        inserted = 0
        updated = 0

        for metric in metrics:
            bind_vars = {
                "categoria": metric.get("categoria"),
                "anio": metric.get("anio"),
                "mes": metric.get("mes"),
                "periodo_tipo": metric.get("periodo_tipo"),
                "metrica_nombre": metric.get("metrica_nombre"),
                "data": metric,
            }

            try:
                cursor = self.db.AQLQuery(
                    upsert_query, bindVars=bind_vars, batchSize=1, rawResults=True
                )
                results = list(cursor) if cursor is not None else []
                result = results[0] if results else None

                if isinstance(result, dict):
                    if result.get("is_inserted"):
                        inserted += 1
                    else:
                        updated += 1
                else:
                    inserted += 1  # fallback en caso de respuesta inesperada

            except Exception as e:
                logger.warning(f"Error al upsert métrica: {e}")
                continue

        total = inserted + updated
        logger.info(f"Upsert completado: {inserted} insertadas, {updated} actualizadas (total {total})")
        return total

    def query_metrics(self, aql_query: str, bind_vars: Optional[Dict] = None) -> List[Dict]:
        """
        Ejecuta una consulta AQL y retorna los resultados.

        Args:
            aql_query: Consulta en ArangoDB Query Language
            bind_vars: Variables para binding en la consulta

        Returns:
            List[Dict]: Lista de resultados
        """
        try:
            if bind_vars is None:
                bind_vars = {}

            aql = self.db.AQLQuery(aql_query, bindVars=bind_vars, rawResults=True)
            results = [doc for doc in aql]
            return results

        except Exception as e:
            logger.error(f"Error al ejecutar query: {e}")
            return []

    def get_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas de la colección metricas_turismo.

        Returns:
            Dict con estadísticas básicas
        """
        try:
            collection = self.db["metricas_turismo"]

            # Total de documentos
            total_docs = collection.count()

            # Categorías únicas
            aql_categorias = """
                FOR doc IN metricas_turismo
                COLLECT categoria = doc.categoria WITH COUNT INTO count
                RETURN {categoria: categoria, count: count}
            """
            categorias = self.query_metrics(aql_categorias)

            # Años cubiertos
            aql_anios = """
                FOR doc IN metricas_turismo
                COLLECT anio = doc.anio
                SORT anio
                RETURN anio
            """
            anios = self.query_metrics(aql_anios)

            return {
                "total_metricas": total_docs,
                "categorias": categorias,
                "anios_cubiertos": anios,
                "ultima_actualizacion": None  # TODO: agregar timestamp
            }

        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {e}")
            return {}

    def clear_collection(self, collection_name: str = "metricas_turismo") -> bool:
        """
        Elimina todos los documentos de una colección (útil para re-ETL).

        Args:
            collection_name: Nombre de la colección a limpiar

        Returns:
            bool: True si se limpió exitosamente
        """
        try:
            logger.warning(f"Limpiando colección '{collection_name}'...")
            aql = f"FOR doc IN {collection_name} REMOVE doc IN {collection_name}"
            self.db.AQLQuery(aql, rawResults=True)
            logger.info(f"Colección '{collection_name}' limpiada exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error al limpiar colección: {e}")
            return False

    def save_qa_interaction(self, question: str, answer: str,
                           query_type: str, aql_query: Optional[str] = None,
                           sources: Optional[List[str]] = None) -> Optional[str]:
        """
        Guarda una interacción de Q&A en el historial.

        Args:
            question: Pregunta del usuario
            answer: Respuesta generada
            query_type: Tipo de query ('sql', 'rag', 'hybrid')
            aql_query: Query AQL ejecutada (si aplica)
            sources: Lista de fuentes de datos utilizadas

        Returns:
            str: ID del documento guardado, None si hubo error
        """
        try:
            from datetime import datetime

            collection = self.db["qa_history"]
            doc_data = {
                "question": question,
                "answer": answer,
                "query_type": query_type,
                "aql_query": aql_query,
                "sources": sources or [],
                "timestamp": datetime.now().isoformat(),
                "system": "nexus"
            }

            doc = collection.createDocument(doc_data)
            doc.save()
            return doc._key

        except Exception as e:
            logger.error(f"Error al guardar interacción Q&A: {e}")
            return None

    def close(self):
        """Cierra la conexión a la base de datos."""
        if self.connection:
            logger.info("Cerrando conexión a ArangoDB")
            self.connection = None
            self.db = None


def initialize_nexus_db() -> Optional[NexusDB]:
    """
    Función helper para inicializar la base de datos Nexus.
    Crea conexión, colecciones e índices.

    Returns:
        NexusDB: Instancia configurada, None si hubo error
    """
    db = NexusDB()

    if not db.connect():
        logger.error("No se pudo conectar a ArangoDB")
        return None

    if not db.initialize_collections():
        logger.error("No se pudieron inicializar las colecciones")
        return None

    if not db.create_indexes():
        logger.warning("Hubo problemas al crear índices (puede que ya existan)")

    logger.info("Base de datos Nexus inicializada correctamente")
    return db


if __name__ == "__main__":
    # Test de conexión y configuración
    print("Inicializando base de datos Nexus...")
    db = initialize_nexus_db()

    if db:
        print("\n✅ Conexión exitosa!")
        stats = db.get_stats()
        print(f"\n📊 Estadísticas:")
        print(f"  - Total métricas: {stats.get('total_metricas', 0)}")
        print(f"  - Años cubiertos: {stats.get('anios_cubiertos', [])}")
        print(f"  - Categorías: {len(stats.get('categorias', []))}")
        db.close()
    else:
        print("\n❌ Error al inicializar la base de datos")
