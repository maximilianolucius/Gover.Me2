#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingesta de noticias en Weaviate (100% local, sin Docker) usando text2vec-transformers.
- Crea (o recrea) la clase News con vectorización via text2vec-transformers.
- Inserta objetos en batch sin "vector" (Weaviate vectoriza automáticamente).
- Lee .json o .json.gz con campos:
    - titulo (str)
    - subtitulo (str|None)
    - autor (str|None)
    - fecha (str ISO-8601 o similar)
    - contenido (list[str] o str)
    - fuente (str|None)
    - url (str|None)
    - depth (int|None)

Variables de entorno:
  WEAVIATE_URL            (default: http://localhost:8080)
  CLASS_NAME              (default: News)
  INPUT_DIR               (default: ./data/news)
  BATCH_SIZE              (default: 64)
  FORCE_RECREATE          (default: 0) -> "1" para borrar y recrear la clase
  USE_WEAVIATE_VECTORIZE  (default: 1) -> "1" para NO enviar "vector" (recomendado)

Uso:
  python ingest_weaviate_news_1.py --input ./ruta/a/noticias
"""

import os
import sys
import json
import gzip
import uuid
import time
import glob
import argparse
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# ------------------- Configuración -------------------

WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")
CLASS_NAME = os.getenv("CLASS_NAME", "News")
NEWS_DIR = os.getenv("NEWS_DIR", "/mnt/disco6tb/Gover.Me/rag_document_data/noticias")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "8"))
FORCE_RECREATE = os.getenv("FORCE_RECREATE", "0") == "1"
USE_WEAVIATE_VECTORIZE = os.getenv("USE_WEAVIATE_VECTORIZE", "1") == "1"
TIMEOUT = (10, 60)  # (connect, read) seconds

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})



FORCE_RECREATE = False


# ------------------- Utilidades -------------------

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)


def read_json_any(path: str) -> Optional[Dict[str, Any]]:
    try:
        if path.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                return json.load(f)
        else:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log(f"⚠️  Error leyendo {path}: {e}")
        return None


def to_iso8601(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    # Intenta parsear varios formatos comunes
    fmts = (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
    )
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            # Si no tiene tz, lo dejamos sin tz (Weaviate admite ISO sin tz)
            return dt.isoformat()
        except Exception:
            pass
    # Como fallback, si ya parece ISO, déjalo pasar
    if "T" in s or "-" in s:
        return s
    return None


def join_body(contenido: Any) -> Optional[str]:
    if contenido is None:
        return None
    if isinstance(contenido, list):
        # Une párrafos con doble salto
        return "\n\n".join([str(x) for x in contenido if x is not None])
    return str(contenido)


def build_id(url: Optional[str], path: str) -> str:
    base = (url or path).strip()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


# ------------------- Esquema -------------------

def get_schema() -> Dict[str, Any]:
    r = session.get(f"{WEAVIATE_URL}/v1/schema", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def delete_class_if_exists(class_name: str) -> None:
    r = session.delete(f"{WEAVIATE_URL}/v1/schema/{class_name}", timeout=TIMEOUT)
    if r.status_code in (200, 204, 404):
        return
    r.raise_for_status()


def class_exists(class_name: str) -> bool:
    schema = get_schema()
    for c in schema.get("classes", []):
        if c.get("class") == class_name:
            return True
    return False


def ensure_schema(class_name: str = CLASS_NAME) -> None:
    """
    Crea (o recrea si FORCE_RECREATE=1) la clase con vectorización via text2vec-transformers.
    Requiere que Weaviate esté arrancado con:
        ENABLE_MODULES="text2vec-transformers,bm25"
        TRANSFORMERS_INFERENCE_API="http://127.0.0.1:8080"
    """
    if class_exists(class_name):
        if FORCE_RECREATE:
            log(f"🧨 Borrando clase existente: {class_name}")
            delete_class_if_exists(class_name)
        else:
            log(f"✅ Clase {class_name} ya existe (no se recrea).")
            return

    log(f"🛠️  Creando clase {class_name} con text2vec-transformers …")
    new_class = {
        "class": class_name,
        "description": "Noticias de prensa (ingesta local)",
        "vectorizer": "text2vec-transformers",
        "vectorIndexType": "hnsw",
        "vectorIndexConfig": {"distance": "cosine"},
        "moduleConfig": {},  # vacío basta si TRANSFORMERS_INFERENCE_API está seteado en el servidor
        "properties": [
            {"name": "title",        "dataType": ["text"]},
            {"name": "subtitle",     "dataType": ["text"]},
            {"name": "author",       "dataType": ["text"]},
            {"name": "publishedAt",  "dataType": ["date"]},
            {"name": "body",         "dataType": ["text"]},
            {"name": "source",       "dataType": ["text"]},
            {"name": "sourceUrl",    "dataType": ["text"]},
            {"name": "depth",        "dataType": ["int"]},
        ],
    }

    r = session.post(f"{WEAVIATE_URL}/v1/schema", data=json.dumps(new_class), timeout=TIMEOUT)
    if r.status_code == 422:
        txt = r.text.lower()
        if "already exists" in txt or "already exists" in r.json().get("error", [{}])[0].get("message", "").lower():
            log(f"ℹ️  Clase {class_name} ya existía (422). Continuando…")
            return
    r.raise_for_status()
    log(f"✅ Clase {class_name} creada.")


# ------------------- Ingesta -------------------

def iter_input_files(input_dir: str) -> Iterable[str]:
    patterns = ["**/*.json", "**/*.json.gz"]
    for pat in patterns:
        for p in glob.iglob(os.path.join(input_dir, pat), recursive=True):
            yield p


def convert_record(raw: Dict[str, Any], path: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Convierte un registro con claves en español al esquema destino.
    Devuelve (id, properties) o None si faltan campos críticos.
    """
    titulo = raw.get("titulo")
    subtitulo = raw.get("subtitulo")
    autor = raw.get("autor")
    fecha = to_iso8601(raw.get("fecha"))
    contenido = join_body(raw.get("contenido"))
    fuente = raw.get("fuente")
    url = raw.get("url")
    depth = raw.get("depth")

    if not titulo and not contenido:
        # Se requiere al menos título o body
        return None

    props = {
        "title": titulo or "",
        "subtitle": subtitulo or "",
        "author": autor or "",
        "publishedAt": fecha,
        "body": contenido or "",
        "source": fuente or "",
        "sourceUrl": url or "",
        "depth": int(depth) if isinstance(depth, int) or (isinstance(depth, str) and depth.isdigit()) else None,
    }

    # Limpia None que rompen validaciones
    props = {k: v for k, v in props.items() if v is not None}

    obj_id = build_id(url, path)
    return obj_id, props


def flush_batch(objs: List[Tuple[str, Dict[str, Any]]]) -> int:
    """
    Envía el batch a Weaviate. No incluye 'vector': dejamos que Weaviate vectorice.
    Soporta respuestas:
      - {"results": {"objects": [...]}}
      - {"objects": [...]}
      - [{...}, {...}]
    """
    if not objs:
        return 0

    payload = {"objects": []}
    for obj_id, props in objs:
        payload["objects"].append({
            "class": CLASS_NAME,
            "id": obj_id,
            "properties": props,
        })

    r = session.post(f"{WEAVIATE_URL}/v1/batch/objects",
                     data=json.dumps(payload),
                     timeout=(10, 120))
    if r.status_code not in (200, 207):
        log(f"❌ Error batch: {r.status_code} {r.text[:500]}")
        r.raise_for_status()

    try:
        data = r.json()
    except Exception:
        log("⚠️  Respuesta de batch no es JSON, asumiendo 0 insertados.")
        return 0

    # Normaliza a una lista de items-resultados
    items = None
    if isinstance(data, dict):
        if "results" in data and isinstance(data["results"], dict) and "objects" in data["results"]:
            items = data["results"]["objects"]
        elif "objects" in data and isinstance(data["objects"], list):
            items = data["objects"]
    elif isinstance(data, list):
        items = data

    inserted = 0
    if isinstance(items, list):
        for it in items:
            # Weaviate suele poner status así:
            # it["status"] == "SUCCESS"   (o)
            # it["result"]["status"] == "SUCCESS"   (según versión)
            status = (it.get("status") or
                      (it.get("result", {}) or {}).get("status"))
            if status and str(status).upper().startswith("SUCCESS"):
                inserted += 1
            else:
                err = (it.get("result", {}) or {}).get("errors") or it.get("errors")
                if err:
                    log(f"⚠️  Error en objeto: {str(err)[:300]}")
    else:
        # Si no tenemos estructura reconocible, como fallback:
        if r.status_code == 200:
            inserted = len(objs)

    return inserted


def ingest_dir(input_dir: str) -> None:
    log(f"📂 Ingestando desde: {input_dir}")
    ensure_schema(CLASS_NAME)

    batch: List[Tuple[str, Dict[str, Any]]] = []
    total_files, total_ok = 0, 0

    for path in iter_input_files(input_dir):
        total_files += 1
        raw = read_json_any(path)
        if not raw:
            continue
        conv = convert_record(raw, path)
        if not conv:
            log(f"↪️  Saltando {path}: sin título ni contenido")
            continue

        batch.append(conv)
        if len(batch) >= BATCH_SIZE:
            ok = flush_batch(batch)
            total_ok += ok
            log(f"✔️  Batch insertado: {ok}/{len(batch)}")
            batch.clear()

    # último batch
    if batch:
        ok = flush_batch(batch)
        total_ok += ok
        log(f"✔️  Batch final insertado: {ok}/{len(batch)}")

    log(f"✅ Ingesta completa. Archivos={total_files} ObjetosOK={total_ok}")


# ------------------- CLI -------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingesta de noticias en Weaviate (text2vec-transformers)")
    p.add_argument("--input", default=NEWS_DIR, help="Directorio con .json/.json.gz (default: %(default)s)")
    p.add_argument("--class", dest="class_name", default=CLASS_NAME, help="Nombre de clase (default: %(default)s)")
    p.add_argument("--batch", dest="batch_size", type=int, default=BATCH_SIZE, help="Batch size (default: %(default)s)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    global CLASS_NAME, BATCH_SIZE
    CLASS_NAME = args.class_name
    BATCH_SIZE = args.batch_size

    # Sanity checks
    if not USE_WEAVIATE_VECTORIZE:
        log("⚠️  USE_WEAVIATE_VECTORIZE=0: este script NO envía 'vector'. Estás deshabilitando la vectorización automática. ¿Seguro?")
    # Optional: chequear que Weaviate está vivo
    try:
        r = session.get(f"{WEAVIATE_URL}/v1/.well-known/ready", timeout=TIMEOUT)
        if r.status_code != 200:
            log(f"⚠️  Weaviate not ready: {r.status_code}")
    except Exception as e:
        log(f"⚠️  No pude verificar readiness de Weaviate: {e}")

    ingest_dir(args.input)


if __name__ == "__main__":
    main()

