#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lista las tablas (y opcionalmente vistas) de la base de datos PostgreSQL
utilizando credenciales del archivo .env del proyecto.

Uso:
  python nexus/list_tables.py                  # Lista tablas del esquema 'public'
  python nexus/list_tables.py --schema other   # Esquema alternativo
  python nexus/list_tables.py --include-views  # Incluir vistas
"""

import os
import argparse
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect


def build_dsn_from_env() -> str:
    load_dotenv()
    dsn_env = os.getenv("NEXUS_DSN")
    if dsn_env:
        return dsn_env
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "nexus")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "password")
    port = os.getenv("POSTGRES_PORT", "5432")
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{db}"
    )


def list_objects(schema: str = "public", include_views: bool = False):
    dsn = build_dsn_from_env()
    engine = create_engine(dsn, pool_pre_ping=True)
    insp = inspect(engine)
    tables = sorted(insp.get_table_names(schema=schema))
    views = sorted(insp.get_view_names(schema=schema)) if include_views else []
    return tables, views


def main() -> int:
    ap = argparse.ArgumentParser(description="Lista tablas de PostgreSQL usando .env")
    ap.add_argument("--schema", default="public", help="Esquema a inspeccionar (default: public)")
    ap.add_argument("--include-views", action="store_true", help="Incluir vistas además de tablas")
    args = ap.parse_args()

    try:
        tables, views = list_objects(schema=args.schema, include_views=args.include_views)
    except Exception as e:
        print(f"Error conectando o inspecionando la BD: {e}")
        return 1

    print(f"Esquema: {args.schema}")
    print(f"Tablas ({len(tables)}):")
    for t in tables:
        print(f"- {t}")

    if args.include_views:
        print(f"\nVistas ({len(views)}):")
        for v in views:
            print(f"- {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

