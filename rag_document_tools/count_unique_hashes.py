#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cuenta hashes únicos (MD5 del campo 'contenido_completo' por defecto)
recorriendo recursivamente un directorio con JSONs (.json y .json.gz).

Uso básico:
  python rag_document_tools/count_unique_hashes.py \
      --dir ./rag_document_data \
      --field contenido_completo \
      --top-dup 10

Considera vacío como cadena "" si el campo no existe o es None.
"""

import os
import json
import gzip
import time
import argparse
import hashlib
from collections import Counter, defaultdict


def compute_hash(text: str) -> str:
    text = text or ""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def read_json(path: str):
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def main():
    ap = argparse.ArgumentParser(description="Cuenta hashes únicos de JSONs por campo")
    ap.add_argument("--dir", "-d", default="./rag_document_data", help="Directorio raíz")
    ap.add_argument("--field", "-f", default="contenido_completo", help="Campo a hashear")
    ap.add_argument("--top-dup", type=int, default=10, help="Top duplicados a mostrar")
    args = ap.parse_args()

    root = args.dir
    field = args.field
    top_dup = max(0, int(args.top_dup))

    if not os.path.exists(root):
        print(f"❌ Directorio no encontrado: {root}")
        return

    t0 = time.perf_counter()
    total_files = 0
    total_json = 0
    errors = 0
    counter = Counter()
    # Para verificación posterior del hash más repetido, guardamos un mapeo parcial
    # Evitamos guardar todo para no consumir demasiada memoria; haremos una segunda pasada

    for cur, _dirs, files in os.walk(root):
        for fn in files:
            if not (fn.endswith(".json") or fn.endswith(".json.gz")):
                continue
            total_files += 1
            path = os.path.join(cur, fn)
            try:
                data = read_json(path)
                total_json += 1
                content = data.get(field, "")
                h = compute_hash(content)
                counter[h] += 1
            except Exception:
                errors += 1

    unique_hashes = len(counter)
    duplicates_files = sum(c - 1 for c in counter.values() if c > 1)
    dt = time.perf_counter() - t0

    print("Resumen:")
    print(f"  Archivos examinados: {total_files}")
    print(f"  JSON válidos:        {total_json}")
    print(f"  Hashes únicos:       {unique_hashes}")
    print(f"  Duplicados (archivos con hash ya visto): {duplicates_files}")
    print(f"  Errores lectura:     {errors}")
    print(f"  Tiempo:              {dt:.2f}s")

    top_hash: str | None = None
    if counter:
        top_hash = max(counter.items(), key=lambda x: x[1])[0]

    if top_dup > 0:
        dup_items = [(h, c) for h, c in counter.items() if c > 1]
        dup_items.sort(key=lambda x: x[1], reverse=True)
        if dup_items:
            print("\nTop duplicados:")
            for i, (h, c) in enumerate(dup_items[:top_dup], 1):
                print(f"  {i:02d}. {h} -> {c} archivos")

    # Verificar que todas las noticias con el hash más repetido sean iguales (por contenido)
    if top_hash:
        print(f"\nVerificación del hash más repetido: {top_hash} (x{counter[top_hash]} archivos)")
        # Segunda pasada para recolectar rutas con ese hash
        files_with_top: list[str] = []
        for cur, _dirs, files in os.walk(root):
            for fn in files:
                if not (fn.endswith(".json") or fn.endswith(".json.gz")):
                    continue
                path = os.path.join(cur, fn)
                try:
                    data = read_json(path)
                    content = data.get(field, "")
                    if compute_hash(content) == top_hash:
                        files_with_top.append(path)
                except Exception:
                    continue

        diffs = 0
        ref_content = None
        for p in files_with_top:
            try:
                data = read_json(p)
                c = data.get(field, "")
                if ref_content is None:
                    ref_content = c
                else:
                    if c != ref_content:
                        diffs += 1
                        if diffs <= 5:
                            print(f"  Diferencia de contenido detectada en: {p}")
            except Exception:
                continue

        if diffs == 0:
            print("  ✅ Todos los contenidos son idénticos para el hash más repetido.")
        else:
            print(f"  ❌ Se encontraron {diffs} archivos con contenido distinto bajo el mismo hash.")


if __name__ == "__main__":
    main()
