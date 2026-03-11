#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verifica si los JSON dados ya existen en la tabla 'documents' (por content_hash).

Calcula MD5(contenido_completo) exactamente igual que el uploader y consulta
en PostgreSQL usando variables de entorno (.env) para la conexión.

Uso:
  python rag_document_tools/check_doc_exists.py path/a.json.gz [b.json ...]

Salida por archivo:
  - content_hash calculado
  - Existe en DB: sí/no
  - Si existe: id, titulo, fecha, url_original
"""

import os
import sys
import json
import gzip
import hashlib
from typing import Optional, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


def load_env_conn():
    load_dotenv()
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "rag_chatbot")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "password")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    return psycopg2.connect(host=host, database=db, user=user, password=pwd, port=port)


def read_json(path: str) -> Optional[Dict]:
    try:
        if path.endswith('.gz'):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                return json.load(f)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"❌ No se pudo leer {path}: {e}")
        return None


def compute_hash(content: str) -> str:
    content = content or ""
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def check_one(cur, path: str):
    data = read_json(path)
    if data is None:
        return
    content = data.get('contenido_completo', '')
    h = compute_hash(content)
    cur.execute("SELECT id, titulo, fecha, url_original FROM documents WHERE content_hash = %s", (h,))
    row = cur.fetchone()
    print(f"\nArchivo: {path}")
    vacio = 'sí' if not (content or '').strip() else 'no'
    print(f"  content_hash: {h}\n  contenido_vacio: {vacio}")
    if row:
        print("  Existe en DB: SÍ")
        print(f"  -> id={row['id']} | fecha={row['fecha']} | titulo={row['titulo']!r}")
        if row.get('url_original'):
            print(f"  -> url={row['url_original']}")
    else:
        print("  Existe en DB: NO")


def main():
    if len(sys.argv) < 2:
        print("Uso: python rag_document_tools/check_doc_exists.py file1.json[.gz] [file2 ...]")
        sys.exit(1)

    try:
        conn = load_env_conn()
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        sys.exit(2)

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for path in sys.argv[1:]:
            check_one(cur, path)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


if __name__ == '__main__':
    main()
