#!/usr/bin/env python3
"""
Backup y limpieza de tablas RAG en PostgreSQL.

Lee la configuración desde variables de entorno (.env) y:
1) Copia las tablas 'documents' y 'text_chunks' a 'documents_copia' y 'text_chunks_copia'.
2) Vacía las tablas originales de forma segura (TRUNCATE ... CASCADE) y reinicia los ID.

Uso:
  python rag_document_tools/pg_backup_and_clear.py

Variables de entorno relevantes (.env):
  POSTGRES_HOST (default: localhost)
  POSTGRES_DB (default: rag_chatbot)
  POSTGRES_USER (default: postgres)
  POSTGRES_PASSWORD (default: password)
  POSTGRES_PORT (default: 5432)
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2


def get_conn():
    load_dotenv()
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "rag_chatbot")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "password")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    return psycopg2.connect(host=host, database=db, user=user, password=pwd, port=port)


def table_exists(cursor, name: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (name,),
    )
    return bool(cursor.fetchone()[0])


def copy_table(cursor, src: str, dst: str):
    if table_exists(cursor, dst):
        cursor.execute(f"DROP TABLE IF EXISTS {dst}")
    cursor.execute(f"CREATE TABLE {dst} AS TABLE {src} WITH DATA")


def count_rows(cursor, name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {name}")
    return int(cursor.fetchone()[0])


def main():
    try:
        conn = get_conn()
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        sys.exit(1)

    try:
        cur = conn.cursor()

        # Copias de seguridad simples
        print("🔄 Creando copias de seguridad de tablas...")
        copy_table(cur, "documents", "documents_copia")
        copy_table(cur, "text_chunks", "text_chunks_copia")

        # Contar antes de limpiar
        docs_before = count_rows(cur, "documents")
        chunks_before = count_rows(cur, "text_chunks")

        # Limpiar tablas originales
        print("🧹 Limpiando tablas originales (TRUNCATE ... CASCADE)...")
        cur.execute("TRUNCATE text_chunks, documents RESTART IDENTITY CASCADE")

        conn.commit()

        print(
            f"✅ Copia realizada y limpieza completada | documents: {docs_before} -> 0 | text_chunks: {chunks_before} -> 0"
        )

    except Exception as e:
        conn.rollback()
        print(f"❌ Error durante la operación: {e}")
        sys.exit(1)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()

