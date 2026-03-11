#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recurre directorios para contar archivos JSON/JSON.GZ que contienen la palabra
"incendio" (singular o plural), sin distinción de mayúsculas/minúsculas.

Uso:
  python rag_document_tools/count_incendio_json.py --root /mnt/disco6tb/Gover.Me/rag_document_data --show

Opciones:
  --root PATH   Directorio raíz a escanear (por defecto: "/mnt/disco6tb/Gover.Me/rag_document_data")
  --show        Muestra las rutas de los JSON que coinciden
"""

import os
import re
import sys
import json
import argparse
import gzip
from typing import Any, Iterable

PATTERN = re.compile(r"\bincendio(s)?\b", re.IGNORECASE)


def iter_strings(obj: Any) -> Iterable[str]:
    """Itera sobre todas las cadenas presentes en un objeto JSON.
    Incluye claves de diccionarios y valores que sean str.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str):
                yield k
            yield from iter_strings(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_strings(item)
    elif isinstance(obj, str):
        yield obj
    else:
        # Ignorar otros tipos para evitar falsos positivos
        return


def json_contains_incendio(path: str) -> bool:
    """Devuelve True si el JSON contiene 'incendio'/'incendios' en alguna
    clave o valor de tipo string. Si el parseo falla, busca en el texto bruto.
    """
    is_gz = path.lower().endswith(".gz")
    # Primer intento: parsear JSON
    try:
        if is_gz:
            with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                data = json.load(f)
        else:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                data = json.load(f)
        for s in iter_strings(data):
            if PATTERN.search(s):
                return True
        return False
    except Exception:
        # Fallback: buscar en texto bruto
        try:
            if is_gz:
                with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                    text = f.read()
            else:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read()
            return bool(PATTERN.search(text))
        except Exception:
            return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Cuenta JSON/JSON.GZ con la palabra 'incendio'")
    ap.add_argument("--root", default="/mnt/disco6tb/Gover.Me/rag_document_data", help="Directorio raíz a escanear")
    ap.add_argument("--show", action="store_true", help="Mostrar rutas de archivos coincidentes")
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    total_json = 0
    matched = 0
    matches = []

    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            lname = name.lower()
            if not (lname.endswith(".json") or lname.endswith(".json.gz")):
                continue
            total_json += 1
            path = os.path.join(dirpath, name)
            if json_contains_incendio(path):
                matched += 1
                if args.show:
                    matches.append(path)

    print(f"Escaneados JSON: {total_json}")
    print(f"Con 'incendio/incendios': {matched}")
    if total_json:
        pct = 100.0 * matched / total_json
        print(f"Porcentaje: {pct:.2f}%")
    if args.show and matches:
        print("\nCoincidencias:")
        for p in matches:
            print(p)

    return 0


if __name__ == "__main__":
    sys.exit(main())
