#!/usr/bin/env python3
"""Pequeño script para auditar las fuentes devueltas por Elysia para una consulta concreta."""

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import patch

from elysia1.elysia_tool import initialize_elysia_tree, make_patched_get_client


def _iter_candidates(obj: Any) -> Iterable[Dict[str, Any]]:
    """Itera sobre posibles diccionarios de artículos dentro del objeto devuelto por Elysia."""
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_candidates(value)
    elif isinstance(obj, (list, tuple, set)):
        for item in obj:
            yield from _iter_candidates(item)


def _extract_articles(objects: Optional[List[Any]]) -> List[Dict[str, Any]]:
    """Extrae artículos relevantes con campos básicos normalizados."""
    results: List[Dict[str, Any]] = []
    if not objects:
        return results

    for obj in objects:
        for candidate in _iter_candidates(obj):
            title = candidate.get("title") or candidate.get("headline")
            if not title:
                continue

            source = candidate.get("source") or candidate.get("publisher") or candidate.get("mediaSource")
            url = (
                candidate.get("sourceUrl")
                or candidate.get("url")
                or candidate.get("link")
            )

            published = (
                candidate.get("published_at")
                or candidate.get("publishedAt")
                or candidate.get("date")
                or candidate.get("publishedDate")
            )

            results.append(
                {
                    "title": str(title).strip(),
                    "source": (str(source).strip() if source else ""),
                    "url": (str(url).strip() if url else ""),
                    "published": (str(published).strip() if published else ""),
                }
            )

    # Eliminar duplicados exactos por título + url
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for article in results:
        key = (article["title"], article["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(article)

    return deduped


def _parse_datetime(value: str) -> Optional[datetime]:
    """Intenta parsear la fecha en varios formatos comunes."""
    value = value.strip()
    if not value:
        return None

    # Fechas ISO completas
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    # Fechas ISO sin zona
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    # Solo fecha
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    return None


def _summarise_articles(articles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Genera métricas básicas de calidad de fuentes."""
    summary: Dict[str, Any] = {
        "total_articles": len(articles),
        "articles": articles,
    }

    if not articles:
        return summary

    sources = [a["source"] or "(sin fuente)" for a in articles]
    summary["unique_sources"] = sorted(set(sources))
    summary["source_counts"] = Counter(sources)

    with_url = sum(1 for a in articles if a["url"])
    summary["articles_with_url"] = with_url

    parsed_dates: List[Tuple[datetime, Dict[str, Any]]] = []
    for article in articles:
        dt = _parse_datetime(article["published"])
        if dt:
            parsed_dates.append((dt, article))

    if parsed_dates:
        parsed_dates.sort(key=lambda x: x[0], reverse=True)
        summary["latest_article"] = {
            "date": parsed_dates[0][0].isoformat(),
            "title": parsed_dates[0][1]["title"],
            "source": parsed_dates[0][1]["source"],
        }
        summary["oldest_article"] = {
            "date": parsed_dates[-1][0].isoformat(),
            "title": parsed_dates[-1][1]["title"],
            "source": parsed_dates[-1][1]["source"],
        }

    return summary


def _to_jsonable(obj: Any) -> Any:
    """Convierte estructuras potencialmente anidadas a algo serializable por JSON."""
    if isinstance(obj, (str, int, float, type(None), bool)):
        return obj
    if isinstance(obj, dict):
        return {str(key): _to_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(item) for item in obj]
    if hasattr(obj, "__dict__"):
        return _to_jsonable(vars(obj))
    return str(obj)


def check_source_quality(question: str, provider: str = "vllm", raw: bool = False) -> Dict[str, Any]:
    """Lanza la pregunta contra Elysia y evalúa las fuentes devueltas."""
    tree = initialize_elysia_tree(provider)

    from elysia.util.client import ClientManager

    with patch.object(ClientManager, "get_client", make_patched_get_client()):
        response, objects = tree(question)

    articles = _extract_articles(objects)
    summary = _summarise_articles(articles)
    summary["response"] = response.strip()
    summary["provider"] = provider
    summary["question"] = question

    if raw:
        summary["raw_objects"] = _to_jsonable(objects)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Audita la calidad de las noticias devueltas por Elysia.")
    parser.add_argument(
        "question",
        nargs="?",
        default="Dime las últimas noticias que traten casos de corrupción del PSOE.",
        help="Pregunta a evaluar"
    )
    parser.add_argument(
        "--provider",
        default="vllm",
        help="Proveedor configurado en Elysia (p. ej. vllm o gemini)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime el resultado en formato JSON (útil para scripting)",
    )
    args = parser.parse_args()

    summary = check_source_quality(args.question, provider=args.provider, raw=args.json)

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    print("Pregunta:", summary["question"])
    print("Proveedor:", summary["provider"])
    print()
    print("Respuesta del asistente:\n", summary["response"])
    print()
    total = summary.get("total_articles", 0)
    print(f"Artículos encontrados: {total}")
    print(f"Artículos con URL: {summary.get('articles_with_url', 0)}")

    source_counts: Counter = summary.get("source_counts", Counter())
    if source_counts:
        print("\nFuentes detectadas (conteo):")
        for source, count in source_counts.most_common():
            print(f"  - {source}: {count}")

    latest = summary.get("latest_article")
    if latest:
        print("\nArtículo más reciente:")
        print(f"  • {latest['date']} — {latest['title']} ({latest['source']})")

    oldest = summary.get("oldest_article")
    if oldest:
        print("Artículo más antiguo:")
        print(f"  • {oldest['date']} — {oldest['title']} ({oldest['source']})")

    if total:
        print("\nListado de artículos (máximo 10):")
        for article in summary["articles"][:10]:
            print(f"- {article['title']}")
            if article['source']:
                print(f"    Fuente: {article['source']}")
            if article['published']:
                print(f"    Fecha:  {article['published']}")
            if article['url']:
                print(f"    URL:    {article['url']}")


if __name__ == "__main__":
    main()

    #  python elysia1/news_quality_check.py --json