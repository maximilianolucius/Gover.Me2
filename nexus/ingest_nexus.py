#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nexus Andalucía Tourism DB - Generalized Ingestion Script
--------------------------------------------------------
Ingest Excel files whose filenames encode scope and period, e.g.:
  01_total_turistas_ene25.xlsx
  02_espanoles_ene25.xlsx
  03_andaluces_ene25.xlsx
  20_malaga_ene25.xlsx
  21_sevilla_ene25.xlsx
  11_cruceros_ene25.xlsx
  12_ciudad_ene25.xlsx
  13_cultural_ene25.xlsx
  09_litoral_ene25.xlsx
  10_interior_ene25.xlsx

What it does
------------
- Parses filename tokens to infer:
    * scope_type: provincia | mercado | segmento | total | desconocido
    * scope_value: normalized token (e.g., "sevilla", "espanoles", "litoral")
    * periodo: yyyy-mm-01 derived from (mes + yy) in the filename
- Reads **all sheets** per Excel and stores each sheet as one row in `staging_raw_files`
  with headers and rows serialized as JSONB.
- Optionally executes your schema SQL (e.g., schema_1.sql) before loading.
- Optionally seeds minimal catalogs (provincias/mercados) if those tables exist.

Requirements
------------
    pip install -r requirements.txt

Quickstart
----------
    python ingest_nexus.py \
      --root /path/to/xlsx \
      --dsn "postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME" \
      --init-schema --schema /path/to/schema_1.sql

    # Or using PG* env vars (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD):
    python ingest_nexus.py --root /path/to/xlsx

Notes
-----
- Upsert semantics on (fname, sheet_name)
- Non-matching filenames are skipped with a warning
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import logging
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime, date, time
import numpy as np

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOG = logging.getLogger("nexus_ingest")

# -------------------------------------------------------------------
# Dictionaries
# -------------------------------------------------------------------

MONTHS_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

PROVINCIAS = {
    "almeria": {"codigo": "04", "nombre": "Almería"},
    "cadiz": {"codigo": "11", "nombre": "Cádiz"},
    "cordoba": {"codigo": "14", "nombre": "Córdoba"},
    "granada": {"codigo": "18", "nombre": "Granada"},
    "huelva": {"codigo": "21", "nombre": "Huelva"},
    "jaen": {"codigo": "23", "nombre": "Jaén"},
    "malaga": {"codigo": "29", "nombre": "Málaga"},
    "sevilla": {"codigo": "41", "nombre": "Sevilla"},
}

MERCADOS = {
    "andaluces": "andaluces",
    "espanoles": "espanoles",
    "resto_espana": "resto_espana",
    "extranjeros": "extranjeros",
    "alemanes": "alemanes",
    "britanicos": "britanicos",
    "otros_mercados": "otros_mercados",
}

SEGMENTOS = {
    "total_turistas": "total_turistas",
    "litoral": "litoral",
    "interior": "interior",
    "cruceros": "cruceros",
    "ciudad": "ciudad",
    "cultural": "cultural",
}

FILENAME_RE = re.compile(
    r"""^(?:
            (?P<prefix>\d{2})_    # optional leading index like 01_
        )?
        (?P<token>[a-zA-Z0-9_]+)  # sevilla|malaga|andaluces|espanoles|total_turistas|...
        _
        (?P<mes>[a-zA-Z]{3})      # month (ES 3 letters): ene feb mar ...
        (?P<yy>\d{2})             # year (2 digits): 25 -> 2025
        \.xlsx$
    """,
    re.VERBOSE | re.IGNORECASE
)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def guess_year(two_digits: int) -> int:
    return 2000 + two_digits  # 00..99 -> 2000..2099

def coalesce_scope(token: str) -> Tuple[str, str]:
    """Return (scope_type, scope_value) based on filename token."""
    t = token.lower()
    if t in PROVINCIAS:
        return "provincia", t
    if t in MERCADOS:
        return "mercado", MERCADOS[t]
    if t in SEGMENTOS:
        return "segmento", SEGMENTOS[t]
    # basic normalization
    t2 = t.replace("-", "_").replace("ó", "o").replace("ñ", "n")
    if t2 in MERCADOS:
        return "mercado", MERCADOS[t2]
    if t2 in SEGMENTOS:
        return "segmento", SEGMENTOS[t2]
    if "total" in t2:
        return "total", "total_turistas"
    return "desconocido", t

def parse_filename(fname: str) -> Dict[str, object]:
    m = FILENAME_RE.match(fname)
    if not m:
        raise ValueError(f"Filename does not match expected pattern: {fname}")
    g = m.groupdict()
    mes = g["mes"].lower()
    if mes not in MONTHS_ES:
        raise ValueError(f"Unknown month '{mes}' in filename: {fname}")
    month = MONTHS_ES[mes]
    year = guess_year(int(g["yy"]))
    token = g["token"]
    scope_type, scope_value = coalesce_scope(token)
    return {
        "prefix": g.get("prefix"),
        "token": token,
        "scope_type": scope_type,
        "scope_value": scope_value,
        "month": month,
        "year": year,
        "periodo": date(year, month, 1),
    }

def get_engine(dsn: Optional[str]) -> Engine:
    if dsn:
        return create_engine(dsn, future=True)
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT,","5432").strip(",")  # tolerate accidental comma
    db   = os.getenv("PGDATABASE", os.getenv("PGDB", "postgres"))
    user = os.getenv("PGUSER", "postgres")
    pwd  = os.getenv("PGPASSWORD", "")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}", future=True)

def run_schema(engine: Engine, schema_sql_path: Path):
    sql_text = schema_sql_path.read_text(encoding="utf-8", errors="ignore")
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
            conn.execute(text(stmt))

def ensure_staging_table(engine: Engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS staging_raw_files (
        id SERIAL PRIMARY KEY,
        fname TEXT NOT NULL,
        scope_type VARCHAR(30) NOT NULL,
        scope_value VARCHAR(100) NOT NULL,
        periodo DATE NOT NULL,
        sheet_name TEXT,
        header_json JSONB,
        data_json JSONB,
        n_rows INTEGER,
        loaded_at TIMESTAMP DEFAULT NOW(),
        UNIQUE (fname, sheet_name)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def ensure_basic_dims(engine: Engine):
    with engine.begin() as conn:
        # provincias
        try:
            for key, info in PROVINCIAS.items():
                conn.execute(text("""
                    INSERT INTO provincias (codigo_provincia, nombre_provincia)
                    VALUES (:cod, :nom)
                    ON CONFLICT (codigo_provincia) DO NOTHING;
                """), {"cod": info["codigo"], "nom": info["nombre"]})
        except Exception:
            pass
        # mercados
        try:
            for code, nm in MERCADOS.items():
                conn.execute(text("""
                    INSERT INTO mercados (codigo_mercado, nombre_mercado)
                    VALUES (:cod, :nom)
                    ON CONFLICT (codigo_mercado) DO NOTHING;
                """), {"cod": code, "nom": nm.title()})
        except Exception:
            pass


def json_safe(v):
    """Convert values to JSON-serializable:
       - pandas/NumPy scalars -> Python scalars
       - dates/datetimes -> ISO 8601 string
       - timedeltas/others -> str(v) as last resort
    """
    try:
        import pandas as pd  # local import for type hints
    except Exception:
        pd = None

    if v is None:
        return None
    # pandas NA
    try:
        import pandas as pd
        if pd is not None and pd.isna(v):
            return None
    except Exception:
        pass

    # numpy scalars -> python types
    try:
        import numpy as np
        if isinstance(v, (np.generic,)):
            return v.item()
    except Exception:
        pass

    # pandas Timestamp / Timedelta
    try:
        import pandas as pd
        if isinstance(v, getattr(pd, "Timestamp", ())):
            return v.isoformat()
        if isinstance(v, getattr(pd, "Timedelta", ())):
            return str(v)
    except Exception:
        pass

    # builtin date/time/datetime
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, time):
        return v.strftime("%H:%M:%S")

    # leave basic JSON-native types as-is
    if isinstance(v, (bool, int, float, str)):
        return v

    # fallback
    return str(v)

def read_all_sheets_as_json(xlsx_path: Path):

    out = {}
    xls = pd.ExcelFile(xlsx_path)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df = df.where(pd.notnull(df), None)
        # ensure JSON-serializable values
        df = df.applymap(json_safe)
        header = [str(c) for c in df.columns.tolist()]
        rows = df.to_dict(orient="records")
        out[str(sheet_name)] = {"header": header, "rows": rows, "n_rows": len(rows)}
    return out

def stage_file(engine: Engine, xlsx_path: Path):
    meta = parse_filename(xlsx_path.name)
    sheets = read_all_sheets_as_json(xlsx_path)

    ins = text("""
        INSERT INTO staging_raw_files
        (fname, scope_type, scope_value, periodo, sheet_name, header_json, data_json, n_rows)
        VALUES (:fname, :scope_type, :scope_value, :periodo, :sheet_name,
                CAST(:header_json AS JSONB), CAST(:data_json AS JSONB), :n_rows)
        ON CONFLICT (fname, sheet_name) DO UPDATE SET
            scope_type = EXCLUDED.scope_type,
            scope_value = EXCLUDED.scope_value,
            periodo = EXCLUDED.periodo,
            header_json = EXCLUDED.header_json,
            data_json = EXCLUDED.data_json,
            n_rows = EXCLUDED.n_rows,
            loaded_at = NOW();
    """)

    count = 0
    with engine.begin() as conn:
        for sheet_name, content in sheets.items():
            payload = {
                "fname": xlsx_path.name,
                "scope_type": meta["scope_type"],
                "scope_value": meta["scope_value"],
                "periodo": meta["periodo"].isoformat(),
                "sheet_name": sheet_name,
                "header_json": json.dumps(content["header"], ensure_ascii=False),
                "data_json": json.dumps(content["rows"], ensure_ascii=False),
                "n_rows": content["n_rows"],
            }
            conn.execute(ins, payload)
            count += 1

    LOG.info("Staged %s sheets from %s (%s=%s %04d-%02d) total_rows=%s",
             count, xlsx_path.name, meta["scope_type"], meta["scope_value"],
             meta["year"], meta["month"],
             sum(sh["n_rows"] for sh in sheets.values()))

def discover_files(root: Path):
    for p in sorted(root.glob("*.xlsx")):
        if FILENAME_RE.match(p.name):
            yield p
        else:
            LOG.warning("Skipped (filename pattern mismatch): %s", p.name)

def main():
    ap = argparse.ArgumentParser(description="Generalized Excel ingestion for Nexus Andalucía -> PostgreSQL (staging).")
    ap.add_argument("--root", type=str, default=".", help="Directory with .xlsx files")
    ap.add_argument("--dsn", type=str, default=None, help="SQLAlchemy DSN (postgresql+psycopg2://user:pass@host:port/db)")
    ap.add_argument("--init-schema", action="store_true", help="Run schema SQL before loading")
    ap.add_argument("--schema", type=str, default=None, help="Path to schema SQL file (required with --init-schema)")
    ap.add_argument("--no-dims", action="store_true", help="Do not seed basic catalogs (provincias/mercados)")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        ap.error(f"--root does not exist: {root}")

    engine = get_engine(args.dsn)

    if args.init_schema:
        if not args.schema:
            ap.error("--init-schema requires --schema /path/to/sql")
        schema_path = Path(args.schema).expanduser().resolve()
        if not schema_path.exists():
            ap.error(f"--schema does not exist: {schema_path}")
        LOG.info("Applying schema: %s", schema_path)
        run_schema(engine, schema_path)

    ensure_staging_table(engine)
    if not args.no_dims:
        ensure_basic_dims(engine)

    files = list(discover_files(root))
    if not files:
        LOG.warning("No valid .xlsx files found in: %s", root)
        return 0

    for fp in files:
        try:
            stage_file(engine, fp)
        except Exception as e:
            LOG.exception("Error staging %s: %s", fp.name, e)

    LOG.info("Ingestion finished. Next step: map staging -> fact/dim tables.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
