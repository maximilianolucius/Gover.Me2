#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recorre un directorio y cuenta JSONs (incluye .json y .json.gz) con
el campo 'contenido_completo' vacío.

Uso:
  python rag_document_tools/count_empty_content_json.py \
      --dir ./rag_document_data \
      --field contenido_completo \
      [--list-empty]

Considera como vacío: None o cadena en blanco (len(strip()) == 0).
"""

import os
import json
import argparse
import gzip
import time
from typing import Tuple


def is_empty(value) -> bool:
    if value is None:
        return True
    try:
        return str(value).strip() == ""
    except Exception:
        return False


def process_file(path: str, field: str) -> Tuple[bool, bool]:
    """Devuelve (es_json_valido, campo_vacio)"""
    try:
        if path.endswith('.gz'):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
        else:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
    except Exception:
        return False, False

    val = data.get(field)
    return True, is_empty(val)


def main():
    ap = argparse.ArgumentParser(description="Cuenta JSONs con 'contenido vacío'")
    ap.add_argument('--dir', '-d', default='./rag_document_data', help='Directorio raíz')
    ap.add_argument('--field', '-f', default='contenido_completo', help='Nombre del campo a evaluar')
    ap.add_argument('--list-empty', action='store_true', help='Listar rutas de archivos vacíos')
    args = ap.parse_args()

    root = args.dir
    field = args.field

    if not os.path.exists(root):
        print(f"❌ Directorio no encontrado: {root}")
        return

    t0 = time.perf_counter()
    total_files = 0
    total_json = 0
    empty_count = 0
    errors = 0
    empty_paths = []

    for cur, _dirs, files in os.walk(root):
        for fn in files:
            if not (fn.endswith('.json') or fn.endswith('.json.gz')):
                continue
            total_files += 1
            path = os.path.join(cur, fn)
            ok, empty = process_file(path, field)
            if not ok:
                errors += 1
                continue
            total_json += 1
            if empty:
                empty_count += 1
                if args.list_empty:
                    empty_paths.append(path)

    dt = time.perf_counter() - t0
    non_empty = total_json - empty_count
    pct_empty = (empty_count / total_json * 100.0) if total_json else 0.0

    print("Resumen:")
    print(f"  Archivos examinados: {total_files}")
    print(f"  JSON válidos:        {total_json}")
    print(f"  Vacíos ({field}):    {empty_count} ({pct_empty:.2f}%)")
    print(f"  No vacíos:           {non_empty}")
    print(f"  Errores lectura:     {errors}")
    print(f"  Tiempo:              {dt:.2f}s")

    if args.list_empty and empty_paths:
        print("\nLista de archivos con campo vacío:")
        for p in empty_paths:
            print(p)


if __name__ == '__main__':
    main()

