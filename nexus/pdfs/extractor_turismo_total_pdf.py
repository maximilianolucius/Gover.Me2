#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extractor de totales turísticos desde PDF ➜ PostgreSQL

Resumen
-------
- Lee 1..N PDFs (o un directorio) con fichas de «TOTAL TURISMO EN ANDALUCÍA»,
  «TURISMO ESPAÑOL EN ANDALUCÍA», «TURISMO EXTRANJERO EN ANDALUCÍA», etc.
- Extrae, vía regex robustas, estos indicadores y los inserta en PostgreSQL:
    * viajeros_hoteles
    * pernoctaciones_hoteles
    * llegadas_aeropuertos (puede venir como N/A)
    * turistas_millones
    * estancia_media_dias
    * gasto_medio_diario
- El período (año/mes) se infiere del nombre del archivo (p. ej. "..._ene23.pdf")
  o de la cadena "Realizado: dd/mm/yyyy" dentro del PDF. También se puede
  forzar con flags --ano / --mes.
- La categoría se intenta inferir del título; si no, se puede pasar con --categoria.

Tabla destino (recomendada)
---------------------------
CREATE TABLE IF NOT EXISTS turismo_total (
    id SERIAL PRIMARY KEY,
    año INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    categoria TEXT NOT NULL,
    viajeros_hoteles NUMERIC,
    pernoctaciones_hoteles NUMERIC,
    llegadas_aeropuertos NUMERIC,
    turistas_millones NUMERIC,
    estancia_media_dias NUMERIC,
    gasto_medio_diario NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
-- Opcional (facilita upserts si desea reemplazar DELETE+INSERT):
-- CREATE UNIQUE INDEX IF NOT EXISTS ux_turismo_total_periodo_cat
--     ON turismo_total(año, mes, categoria);

Dependencias
------------
 pip install pdfminer.six psycopg2-binary

Uso
----
 python extractor_turismo_pdf_postgres.py \
   --input ./nexus/data/pdfs/ene2023 \
   --categoria total_andalucia \
   --db-host 0.0.0.0 --db-name nexus --db-user maxim --db-pass diganDar --db-port 5432

# Si el mes/año no se pueden inferir del PDF o del nombre del archivo:
 python extractor_turismo_pdf_postgres.py --input ./ficha.pdf --ano 2023 --mes 1 --categoria total_andalucia

Notas
-----
- Por defecto hace DELETE+INSERT (idempotente). Si activa --use-upsert y ya
  existe un índice único (año,mes,categoria), utilizará ON CONFLICT.
"""

from __future__ import annotations

import os
import re
import sys
from typing import Dict, Optional, Tuple, Iterable, List

from pdfminer.high_level import extract_text
import psycopg2

# ===================== CONFIGURACIÓN (sin argumentos) =====================
# Edita estas variables antes de ejecutar el script
INPUT_PATH = "./nexus/data/ultimos_datos_turisticos/"   # Ruta a un PDF o a una carpeta con PDFs
CATEGORIA = None  # "total_andalucia" | "total_espana" | "total_extranjeros" | None para inferir del PDF
ANO = None        # int o None (si None, se infiere por nombre de archivo o texto)
MES = None        # 1..12 o None

PRINT_ONLY = False   # True = no inserta, solo imprime; False = inserta en PostgreSQL
USE_UPSERT = False  # Requiere índice único (año,mes,categoria)

# Credenciales de PostgreSQL
DB_HOST = "0.0.0.0"
DB_NAME = "nexus"
DB_USER = "maxim"
DB_PASS = "diganDar"
DB_PORT = 5432
# ========================================================================

SPANISH_MONTHS = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

TITLE_TO_CATEGORY = {
    # heurísticas por encabezado
    "TOTAL TURISMO EN ANDALUCÍA": "total_andalucia",
    "TURISMO ESPAÑOL EN ANDALUCÍA": "total_espana",
    "TURISMO EXTRANJERO EN ANDALUCÍA": "total_extranjeros",
}

# Regex para capturar indicadores. Son tolerantes a variaciones de espacios/acentos.
PATTERNS = {
    "viajeros_hoteles": re.compile(
        r"Número\s+de\s+viajeros\s+en\s+establecimientos\s+hoteleros\s*:\s*([\d\.,]+)",
        re.IGNORECASE),
    "pernoctaciones_hoteles": re.compile(
        r"Número\s+de\s+pernoctaciones\s+en\s+establecimientos\s+hoteleros\s*:\s*([\d\.,]+)",
        re.IGNORECASE),
    "llegadas_aeropuertos": re.compile(
        r"Llegadas\s+de\s+pasajeros\s+a\s+aeropuertos\s+andaluces\s*:\s*(N/?A|[\d\.,]+)",
        re.IGNORECASE),
    "turistas_millones": re.compile(
        r"Número\s+de\s+turistas\s*\(millones\)\s*:\s*([\d\.,]+)",
        re.IGNORECASE),
    "estancia_media_dias": re.compile(
        r"Estancia\s+Media\s*\(número\s+de\s+días\)\s*:\s*([\d\.,]+)",
        re.IGNORECASE),
    "gasto_medio_diario": re.compile(
        r"Gasto\s+medio\s+diario\s*\(euros\)\s*:\s*([\d\.,]+)",
        re.IGNORECASE),
}

REALIZADO_RE = re.compile(r"Realizado\s*:\s*(\d{2})/(\d{2})/(\d{4})")
FILENAME_MMYY_RE = re.compile(r"(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)(\d{2})", re.IGNORECASE)


def parse_number(value: str) -> Optional[float]:
    """Convierte números con formato español (puntos de miles, coma decimal) a float.
    Devuelve None para N/A o vacío.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.upper() in {"N/A", "NA"}:
        return None
    # Quitar % si hay
    s = s.replace("%", "").strip()
    # Quitar separadores de miles (.) y cambiar coma decimal por punto
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def infer_period_from_filename(path: str) -> Optional[Tuple[int, int]]:
    name = os.path.basename(path)
    m = FILENAME_MMYY_RE.search(name)
    if not m:
        return None
    mm_txt, yy = m.group(1).lower(), int(m.group(2))
    mes = SPANISH_MONTHS.get(mm_txt)
    if not mes:
        return None
    año = 2000 + yy
    return año, mes


def infer_period_from_text(text: str) -> Optional[Tuple[int, int]]:
    m = REALIZADO_RE.search(text)
    if not m:
        return None
    dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    # La fecha "Realizado" es la fecha de emisión del informe. Si el día <= 10,
    # suele corresponder al mes anterior. Heurística configurable:
    mes = max(1, mm - 1) if dd <= 10 else mm
    año = yyyy if not (dd <= 10 and mm == 1) else yyyy - 1
    return año, mes


def infer_category(text: str, fallback: Optional[str]) -> Optional[str]:
    for title, cat in TITLE_TO_CATEGORY.items():
        if title.lower() in text.lower():
            return cat
    return fallback


def extract_indicators(text: str) -> Dict[str, Optional[float]]:
    data = {k: None for k in PATTERNS.keys()}
    for key, rx in PATTERNS.items():
        m = rx.search(text)
        if m:
            data[key] = parse_number(m.group(1))
    return data


def yield_input_files(input_path: str) -> Iterable[str]:
    if os.path.isdir(input_path):
        for root, _dirs, files in os.walk(input_path):
            for f in files:
                if f.lower().endswith(".pdf"):
                    yield os.path.join(root, f)
    else:
        yield input_path


def delete_then_insert(conn, registro: Dict):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM turismo_total WHERE año=%s AND mes=%s AND categoria=%s",
            (registro["año"], registro["mes"], registro["categoria"]))
        cur.execute(
            """
            INSERT INTO turismo_total (
                año, mes, categoria,
                viajeros_hoteles, pernoctaciones_hoteles, llegadas_aeropuertos,
                turistas_millones, estancia_media_dias, gasto_medio_diario
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                registro["año"], registro["mes"], registro["categoria"],
                registro.get("viajeros_hoteles"),
                registro.get("pernoctaciones_hoteles"),
                registro.get("llegadas_aeropuertos"),
                registro.get("turistas_millones"),
                registro.get("estancia_media_dias"),
                registro.get("gasto_medio_diario"),
            ),
        )


def upsert_on_conflict(conn, registro: Dict):
    """Requiere índice único (año,mes,categoria)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO turismo_total (
                año, mes, categoria,
                viajeros_hoteles, pernoctaciones_hoteles, llegadas_aeropuertos,
                turistas_millones, estancia_media_dias, gasto_medio_diario
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (año, mes, categoria) DO UPDATE SET
                viajeros_hoteles=EXCLUDED.viajeros_hoteles,
                pernoctaciones_hoteles=EXCLUDED.pernoctaciones_hoteles,
                llegadas_aeropuertos=EXCLUDED.llegadas_aeropuertos,
                turistas_millones=EXCLUDED.turistas_millones,
                estancia_media_dias=EXCLUDED.estancia_media_dias,
                gasto_medio_diario=EXCLUDED.gasto_medio_diario,
                updated_at=NOW()
            """,
            (
                registro["año"], registro["mes"], registro["categoria"],
                registro.get("viajeros_hoteles"),
                registro.get("pernoctaciones_hoteles"),
                registro.get("llegadas_aeropuertos"),
                registro.get("turistas_millones"),
                registro.get("estancia_media_dias"),
                registro.get("gasto_medio_diario"),
            ),
        )


def main() -> int:
    files = list(yield_input_files(INPUT_PATH))
    if not files:
        print("No se encontraron PDFs para procesar.")
        return 1

    conn = None
    try:
        if not PRINT_ONLY:
            conn = psycopg2.connect(
                host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
            )
            conn.autocommit = False

        processed = 0
        pdf_path = files[0]
        for pdf_path in files:
            print(f"\n=== Procesando: {pdf_path}")
            try:
                text = extract_text(pdf_path) or ""
            except Exception as e:
                print(f"  Error extrayendo texto: {e}")
                continue

            # data = extract_indicators(text)
            #
            # categoria = infer_category(text, CATEGORIA)
            # if not categoria:
            #     print("  ⚠️  No se pudo inferir 'categoria'. Ajusta CATEGORIA en configuración.")
            #     continue

            año, mes = ANO, MES
            if año is None or mes is None:
                fm = infer_period_from_filename(pdf_path)
                if fm:
                    año, mes = fm
                else:
                    tm = infer_period_from_text(text)
                    if tm:
                        año, mes = tm

            if año is None or mes is None:
                print("  ⚠️  No se pudo inferir período (año/mes). Ajusta ANO/MES en configuración.")
                continue

            #     ----
            from nexus.pdfs.extractor_turismo_total_pdf_0 import get_data

            data_collector = get_data(pdf_path)

            for categoria, data in data_collector.items():

                registro = {
                    "año": año,
                    "mes": mes,
                    "categoria": categoria,
                    **data,
                }

                print("  Registro:")
                for k in [
                    "año", "mes", "categoria",
                    "viajeros_hoteles", "pernoctaciones_hoteles", "llegadas_aeropuertos",
                    "turistas_millones", "estancia_media_dias", "gasto_medio_diario",
                ]:
                    print(f"    - {k}: {registro.get(k)}")

                if PRINT_ONLY:
                    processed += 1
                    continue

                try:
                    if USE_UPSERT:
                        upsert_on_conflict(conn, registro)
                    else:
                        delete_then_insert(conn, registro)
                    processed += 1
                except Exception as e:
                    print(f"  ❌ Error insertando: {e}")
                    if conn:
                        conn.rollback()
                else:
                    if conn:
                        conn.commit()

        print(f"\n✅ Terminado. Registros procesados: {processed}")
        return 0
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
