#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para buscar en Weaviate por eventos culturales en Sevilla.
Realiza búsqueda híbrida (vectorial + keyword) para obtener mejores resultados.

Uso:
    python search_weaviate_eventos.py
    python search_weaviate_eventos.py --query "eventos culturales sevilla"
    python search_weaviate_eventos.py --limit 5
"""

import os
import sys
import json
import argparse
from typing import Any, Dict, List, Optional

import requests

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    SentenceTransformer = None

# ------------------- Configuración -------------------

WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")
CLASS_NAME = os.getenv("CLASS_NAME", "News")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "jinaai/jina-embeddings-v2-base-es")
EMBEDDING_DEVICE = os.getenv("EMBEDDING_DEVICE", "cuda")
DEFAULT_LIMIT = 10
TIMEOUT = (10, 30)

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

_EMBED_MODEL: Optional[SentenceTransformer] = None


# ------------------- Utilidades -------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def resolve_embedding_device() -> str:
    """Determina el dispositivo a usar para embeddings."""
    try:
        import torch
        if torch.cuda.is_available() and EMBEDDING_DEVICE.lower() in ("cuda", "gpu"):
            return "cuda"
    except ImportError:
        pass
    return "cpu"


def get_embedding_model() -> SentenceTransformer:
    """Carga el modelo de embeddings (lazy loading)."""
    global _EMBED_MODEL
    if _EMBED_MODEL is not None:
        return _EMBED_MODEL

    if SentenceTransformer is None:
        raise RuntimeError(
            "sentence-transformers no está instalado. "
            "Instala con: pip install sentence-transformers"
        )

    device = resolve_embedding_device()
    try:
        model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)
        _EMBED_MODEL = model
        log(f"Modelo de embeddings cargado en {device}: {EMBEDDING_MODEL_NAME}")
        return model
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo cargar el modelo de embeddings ({EMBEDDING_MODEL_NAME}): {exc}"
        ) from exc


def generate_embedding(text: str) -> List[float]:
    """Genera el embedding vectorial para un texto."""
    model = get_embedding_model()
    try:
        vector = model.encode(text, convert_to_numpy=True, show_progress_bar=False)
        return vector.tolist()
    except Exception as exc:
        log(f"Error generando embedding: {exc}")
        raise


def check_weaviate_connection() -> bool:
    """Verifica que Weaviate esté disponible."""
    try:
        r = session.get(f"{WEAVIATE_URL}/v1/.well-known/ready", timeout=TIMEOUT)
        if r.status_code == 200:
            log(f"Conectado a Weaviate en {WEAVIATE_URL}")
            return True
        else:
            log(f"Weaviate no está listo: {r.status_code}")
            return False
    except Exception as e:
        log(f"No se pudo conectar a Weaviate: {e}")
        return False


def check_class_exists() -> bool:
    """Verifica que la clase News exista en Weaviate."""
    try:
        r = session.get(f"{WEAVIATE_URL}/v1/schema", timeout=TIMEOUT)
        r.raise_for_status()
        schema = r.json()
        classes = [c.get("class") for c in schema.get("classes", [])]
        if CLASS_NAME in classes:
            log(f"Clase '{CLASS_NAME}' encontrada en Weaviate")
            return True
        else:
            log(f"Clase '{CLASS_NAME}' no encontrada. Clases disponibles: {classes}")
            return False
    except Exception as e:
        log(f"Error verificando esquema: {e}")
        return False


# ------------------- Búsqueda -------------------

def search_hybrid(query: str, limit: int = DEFAULT_LIMIT, alpha: float = 0.5) -> List[Dict[str, Any]]:
    """
    Realiza una búsqueda híbrida en Weaviate (vector + keyword).

    Args:
        query: Texto de búsqueda
        limit: Número máximo de resultados
        alpha: Balance entre búsqueda vectorial (1.0) y keyword (0.0). Default: 0.5

    Returns:
        Lista de resultados con sus propiedades y scores
    """
    log(f"\nBuscando: '{query}' (limit={limit}, alpha={alpha})")

    # Generar embedding para la query
    try:
        query_vector = generate_embedding(query)
    except Exception as e:
        log(f"Error generando embedding para la query: {e}")
        return []

    # Construir GraphQL query para búsqueda híbrida
    graphql_query = """
    {
      Get {
        %s(
          hybrid: {
            query: "%s"
            vector: %s
            alpha: %s
          }
          limit: %d
        ) {
          title
          subtitle
          body
          author
          publishedAt
          source
          sourceUrl
          topics
          axisSlugs
          sentimentScore
          _additional {
            score
            id
          }
        }
      }
    }
    """ % (
        CLASS_NAME,
        query.replace('"', '\\"'),
        json.dumps(query_vector),
        alpha,
        limit
    )

    payload = {"query": graphql_query}

    try:
        r = session.post(
            f"{WEAVIATE_URL}/v1/graphql",
            data=json.dumps(payload),
            timeout=TIMEOUT
        )
        r.raise_for_status()
        result = r.json()

        if "errors" in result:
            log(f"Errores en la consulta GraphQL: {result['errors']}")
            return []

        items = result.get("data", {}).get("Get", {}).get(CLASS_NAME, [])
        return items

    except Exception as e:
        log(f"Error realizando búsqueda: {e}")
        return []


def search_keyword(query: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
    """
    Realiza una búsqueda por keyword en Weaviate usando BM25.

    Args:
        query: Texto de búsqueda
        limit: Número máximo de resultados

    Returns:
        Lista de resultados con sus propiedades y scores
    """
    log(f"\nBuscando (keyword): '{query}' (limit={limit})")

    graphql_query = """
    {
      Get {
        %s(
          bm25: {
            query: "%s"
          }
          limit: %d
        ) {
          title
          subtitle
          body
          author
          publishedAt
          source
          sourceUrl
          topics
          axisSlugs
          sentimentScore
          _additional {
            score
            id
          }
        }
      }
    }
    """ % (CLASS_NAME, query.replace('"', '\\"'), limit)

    payload = {"query": graphql_query}

    try:
        r = session.post(
            f"{WEAVIATE_URL}/v1/graphql",
            data=json.dumps(payload),
            timeout=TIMEOUT
        )
        r.raise_for_status()
        result = r.json()

        if "errors" in result:
            log(f"Errores en la consulta GraphQL: {result['errors']}")
            return []

        items = result.get("data", {}).get("Get", {}).get(CLASS_NAME, [])
        return items

    except Exception as e:
        log(f"Error realizando búsqueda: {e}")
        return []


def search_vector(query: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
    """
    Realiza una búsqueda vectorial pura en Weaviate.

    Args:
        query: Texto de búsqueda
        limit: Número máximo de resultados

    Returns:
        Lista de resultados con sus propiedades y scores
    """
    log(f"\nBuscando (vector): '{query}' (limit={limit})")

    try:
        query_vector = generate_embedding(query)
    except Exception as e:
        log(f"Error generando embedding para la query: {e}")
        return []

    graphql_query = """
    {
      Get {
        %s(
          nearVector: {
            vector: %s
          }
          limit: %d
        ) {
          title
          subtitle
          body
          author
          publishedAt
          source
          sourceUrl
          topics
          axisSlugs
          sentimentScore
          _additional {
            distance
            id
          }
        }
      }
    }
    """ % (CLASS_NAME, json.dumps(query_vector), limit)

    payload = {"query": graphql_query}

    try:
        r = session.post(
            f"{WEAVIATE_URL}/v1/graphql",
            data=json.dumps(payload),
            timeout=TIMEOUT
        )
        r.raise_for_status()
        result = r.json()

        if "errors" in result:
            log(f"Errores en la consulta GraphQL: {result['errors']}")
            return []

        items = result.get("data", {}).get("Get", {}).get(CLASS_NAME, [])
        return items

    except Exception as e:
        log(f"Error realizando búsqueda: {e}")
        return []


# ------------------- Formateo de resultados -------------------

def truncate_text(text: Optional[str], max_length: int = 200) -> str:
    """Trunca un texto largo para mostrar."""
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


def format_result(item: Dict[str, Any], index: int) -> str:
    """Formatea un resultado individual para mostrar."""
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"Resultado #{index + 1}")
    lines.append(f"{'='*80}")

    # Información básica
    title = item.get("title", "Sin título")
    lines.append(f"Título: {title}")

    subtitle = item.get("subtitle")
    if subtitle:
        lines.append(f"Subtítulo: {subtitle}")

    author = item.get("author")
    if author:
        lines.append(f"Autor: {author}")

    published_at = item.get("publishedAt")
    if published_at:
        lines.append(f"Fecha: {published_at}")

    source = item.get("source")
    if source:
        lines.append(f"Fuente: {source}")

    source_url = item.get("sourceUrl")
    if source_url:
        lines.append(f"URL: {source_url}")

    # Metadata adicional
    topics = item.get("topics")
    if topics:
        topics_str = ", ".join(topics) if isinstance(topics, list) else str(topics)
        lines.append(f"Temas: {topics_str}")

    axes = item.get("axisSlugs")
    if axes:
        axes_str = ", ".join(axes) if isinstance(axes, list) else str(axes)
        lines.append(f"Ejes: {axes_str}")

    sentiment = item.get("sentimentScore")
    if sentiment is not None:
        lines.append(f"Sentimiento: {sentiment:.3f}")

    # Score de relevancia
    additional = item.get("_additional", {})
    score = additional.get("score")
    distance = additional.get("distance")
    if score is not None:
        lines.append(f"Score de relevancia: {score}")
    elif distance is not None:
        lines.append(f"Distancia vectorial: {distance}")

    obj_id = additional.get("id")
    if obj_id:
        lines.append(f"ID: {obj_id}")

    # Contenido (truncado)
    body = item.get("body")
    if body:
        lines.append(f"\nContenido (extracto):")
        lines.append(truncate_text(body, 300))

    return "\n".join(lines)


def print_results(results: List[Dict[str, Any]]) -> None:
    """Imprime todos los resultados formateados."""
    if not results:
        log("\nNo se encontraron resultados.")
        return

    log(f"\nSe encontraron {len(results)} resultados:\n")

    for idx, item in enumerate(results):
        print(format_result(item, idx))

    print(f"\n{'='*80}\n")
    log(f"Total de resultados mostrados: {len(results)}")


# ------------------- CLI -------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Buscar eventos culturales en Sevilla en Weaviate"
    )
    p.add_argument(
        "--query", "-q",
        default="eventos culturales sevilla",
        help="Texto de búsqueda (default: 'eventos culturales sevilla')"
    )
    p.add_argument(
        "--limit", "-l",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Número máximo de resultados (default: {DEFAULT_LIMIT})"
    )
    p.add_argument(
        "--method", "-m",
        choices=["hybrid", "keyword", "vector"],
        default="hybrid",
        help="Método de búsqueda (default: hybrid)"
    )
    p.add_argument(
        "--alpha", "-a",
        type=float,
        default=0.5,
        help="Alpha para búsqueda híbrida: 1.0=solo vector, 0.0=solo keyword (default: 0.5)"
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Mostrar resultados en formato JSON"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Verificar conexión
    if not check_weaviate_connection():
        log("Error: No se pudo conectar a Weaviate. Asegúrate de que esté ejecutándose.")
        sys.exit(1)

    # Verificar que existe la clase
    if not check_class_exists():
        log(f"Error: La clase '{CLASS_NAME}' no existe en Weaviate.")
        sys.exit(1)

    # Realizar búsqueda según el método seleccionado
    if args.method == "hybrid":
        results = search_hybrid(args.query, args.limit, args.alpha)
    elif args.method == "keyword":
        results = search_keyword(args.query, args.limit)
    elif args.method == "vector":
        results = search_vector(args.query, args.limit)
    else:
        log(f"Método desconocido: {args.method}")
        sys.exit(1)

    # Mostrar resultados
    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        print_results(results)


if __name__ == "__main__":
    main()
