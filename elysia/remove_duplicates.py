#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Eliminar duplicados de JSON por URL o por hash de contenido.

Características:
- Recursivo por defecto (subdirectorios).
- Modo 'hash' (MD5 del campo, por defecto 'contenido_completo') o 'url'.
- Política de conservación: 'date' (fecha del artículo) o 'mtime' (archivo más reciente).
- Opción de mover duplicados a un directorio en lugar de borrarlos.

Ejemplos:
  # Deduplicar por hash, conservar por fecha de artículo, mover duplicados
  python rag_document_tools/remove_duplicates.py \
      --dir ./rag_document_data \
      --mode hash --field contenido_completo \
      --policy date --move-to ./rag_document_data_duplicados -y

  # Deduplicar por URL, borrar duplicados directamente
  python rag_document_tools/remove_duplicates.py --dir ./rag_document_data --mode url -y
"""

import os
import json
import gzip
import argparse
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


def _iter_json_files(root: str) -> List[str]:
    files: List[str] = []
    for cur, _dirs, fnames in os.walk(root):
        for fn in fnames:
            if fn.endswith('.json') or fn.endswith('.json.gz'):
                files.append(os.path.join(cur, fn))
    return files


def _read_json(path: str) -> Tuple[Dict, str]:
    if path.endswith('.json.gz'):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            data = json.load(f)
    else:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    return data, ('gz' if path.endswith('.gz') else 'json')


def _md5(text: str) -> str:
    text = text or ''
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def _to_ts(dt_str: str) -> float:
    if not dt_str:
        return 0.0
    try:
        dt = datetime.fromisoformat(str(dt_str).replace('Z', '+00:00'))
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.timestamp()
    except Exception:
        return 0.0


def _safe_move(src: str, dst_dir: str):
    os.makedirs(dst_dir, exist_ok=True)
    base = os.path.basename(src)
    target = os.path.join(dst_dir, base)
    if not os.path.exists(target):
        shutil.move(src, target)
        return target
    stem = Path(base).stem
    suffix = ''.join(Path(base).suffixes)
    i = 1
    while True:
        cand = os.path.join(dst_dir, f"{stem}__dup{i}{suffix}")
        if not os.path.exists(cand):
            shutil.move(src, cand)
            return cand
        i += 1


def remove_duplicates(
    directory_path: str = "../rag_document_data",
    mode: str = 'hash',
    field: str = 'contenido_completo',
    policy: str = 'date',
    move_to: str | None = None,
    assume_yes: bool = True,
    dry_run: bool = False,
):
    files = _iter_json_files(directory_path)
    if not files:
        print(f"No se encontraron archivos JSON en {directory_path}")
        return

    key_to_files: Dict[str, List[Tuple[str, float, float]]] = {}
    errors: List[str] = []

    for path in files:
        try:
            data, _fmt = _read_json(path)
            if mode == 'url':
                key = data.get('url_original') or ''
            else:
                key = _md5(data.get(field) or '')

            if not key:
                errors.append(f"Clave vacía en: {path}")
                continue

            mtime = os.path.getmtime(path)
            art_ts = _to_ts(data.get('fecha')) if policy == 'date' else 0.0
            key_to_files.setdefault(key, []).append((path, art_ts, mtime))

        except Exception as e:
            errors.append(f"Error leyendo {path}: {e}")

    duplicates: List[str] = []
    kept: Dict[str, str] = {}
    for key, entries in key_to_files.items():
        if len(entries) == 1:
            kept[key] = entries[0][0]
            continue

        # Selección según política
        if policy == 'date':
            entries.sort(key=lambda t: (t[1], t[2]), reverse=True)  # primero fecha artículo, luego mtime
        else:  # mtime
            entries.sort(key=lambda t: t[2], reverse=True)

        keep_path = entries[0][0]
        kept[key] = keep_path
        to_process = [p for (p, _, _) in entries[1:]]
        for p in to_process:
            print(f"Duplicado: {Path(p).name} (mantener: {Path(keep_path).name})")
        duplicates.extend(to_process)

    removed = 0
    moved = 0
    if duplicates:
        if assume_yes:
            confirm_yes = True
        else:
            action = 'mover' if move_to else 'eliminar'
            confirm = input(f"\n¿{action.capitalize()} {len(duplicates)} archivos duplicados? (s/N): ")
            confirm_yes = confirm.lower() in ['s', 'si', 'y', 'yes']

        if confirm_yes and not dry_run:
            for p in duplicates:
                try:
                    if move_to:
                        _safe_move(p, move_to)
                        moved += 1
                    else:
                        os.remove(p)
                        removed += 1
                except Exception as e:
                    errors.append(f"Error procesando {p}: {e}")

    # Reporte
    total = len(files)
    uniques = sum(1 for v in key_to_files.values() if len(v) >= 1)
    print("\n📊 Resumen:")
    print(f"   Archivos escaneados: {total}")
    print(f"   Clave: {'URL' if mode=='url' else 'HASH('+field+')'}")
    print(f"   Entradas únicas: {len(key_to_files)}")
    print(f"   Conservados: {len(kept)}")
    print(f"   Duplicados {('movidos' if move_to else 'eliminados')}: {moved if move_to else removed}")
    print(f"   Errores: {len(errors)}")
    if errors:
        for e in errors[:5]:
            print(f"   - {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eliminar/mover duplicados de JSON por URL o HASH")
    parser.add_argument("--dir", default="../rag_document_data", help="Directorio raíz (recursivo)")
    parser.add_argument("--mode", choices=["url", "hash"], default="hash", help="Modo de deduplicación")
    parser.add_argument("--field", default="contenido_completo", help="Campo para hash en modo hash")
    parser.add_argument("--policy", choices=["date", "mtime"], default="date", help="Criterio para conservar")
    parser.add_argument("--move-to", default=None, help="Mover duplicados a este directorio (si no, borrar)")
    parser.add_argument("-y", "--yes", action="store_true", help="No pedir confirmación (por defecto no pide)")
    parser.add_argument("--ask", action="store_true", help="Pedir confirmación antes de eliminar/mover")
    parser.add_argument("--dry-run", action="store_true", help="No modificar, solo listar")

    args = parser.parse_args()
    remove_duplicates(
        directory_path=args.dir,
        mode=args.mode,
        field=args.field,
        policy=args.policy,
        move_to=args.move_to,
        assume_yes=(args.yes or (not args.ask)),
        dry_run=args.dry_run,
    )
