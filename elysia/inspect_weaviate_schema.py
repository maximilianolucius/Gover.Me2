#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Herramienta CLI para inspeccionar atributos de clases en Weaviate."""

import argparse
import json
import os
import sys
from typing import Any, Dict, List

import requests

from elysia1.elysia_tool import _load_env_defaults, resolve_weaviate_cfg


def fetch_schema() -> Dict[str, Any]:
    """Recupera el esquema completo de Weaviate como diccionario."""
    _load_env_defaults()
    cfg = resolve_weaviate_cfg()

    base_url = cfg["weaviate_url"].rstrip("/")
    headers = {"Content-Type": "application/json"}
    if cfg["weaviate_api_key"]:
        headers["Authorization"] = f"Bearer {cfg['weaviate_api_key']}"

    resp = requests.get(f"{base_url}/v1/schema", headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def extract_class(schema: Dict[str, Any], class_name: str) -> Dict[str, Any]:
    """Devuelve la definición de clase solicitada o lanza ValueError."""
    classes: List[Dict[str, Any]] = schema.get("classes", [])
    for cls in classes:
        if cls.get("class") == class_name:
            return cls
    available = ", ".join(sorted(cls.get("class", "") for cls in classes))
    raise ValueError(
        f"No se encontró la clase '{class_name}'. Clases disponibles: {available or 'ninguna'}"
    )


def dump_class_info(cls_def: Dict[str, Any]) -> None:
    """Imprime en stdout los atributos y metadatos principales de la clase."""
    class_name = cls_def.get("class", "(sin nombre)")
    vectorizer = cls_def.get("vectorizer", "(desconocido)")
    module_config = cls_def.get("moduleConfig", {})

    print(f"Clase: {class_name}")
    print(f"Vectorizer: {vectorizer}")
    if module_config:
        print("Module config:")
        print(json.dumps(module_config, indent=2, ensure_ascii=False))

    properties: List[Dict[str, Any]] = cls_def.get("properties", [])
    if not properties:
        print("\nNo se encontraron propiedades.")
        return

    print("\nPropiedades:")
    for prop in properties:
        name = prop.get("name", "(sin nombre)")
        data_type = ", ".join(prop.get("dataType", [])) or "(sin tipo)"
        description = prop.get("description", "")
        print(f"  - {name}: {data_type}")
        if description:
            print(f"      descripción: {description}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspecciona las propiedades de una clase en Weaviate."
    )
    parser.add_argument(
        "class_name",
        nargs="?",
        default=os.getenv("WEAVIATE_NEWS_CLASS", "News"),
        help="Nombre de la clase a inspeccionar (por defecto: %(default)s).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        schema = fetch_schema()
        cls_def = extract_class(schema, args.class_name)
        dump_class_info(cls_def)
    except requests.RequestException as exc:
        print(f"[ERROR] fallo al consultar el esquema de Weaviate: {exc}", file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
