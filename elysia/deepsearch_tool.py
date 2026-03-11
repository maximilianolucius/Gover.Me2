#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Herramienta LangChain que utiliza el motor DeepSearch interno para realizar
búsquedas web con múltiples iteraciones y devolver un resumen en español con
fuentes verificables. Sustituye la antigua integración con Gemini.
"""

from __future__ import annotations

from dotenv import load_dotenv

# Cargamos variables de entorno que puedan contener credenciales o ajustes.
load_dotenv("elysia/.elysia_env")
load_dotenv(".env")
load_dotenv()

import asyncio
import logging
from threading import Lock
from typing import Any, Dict, List, Optional

from langchain_core.tools import BaseTool
from pydantic import Field

try:
    from deepsearcher.adaptive_deepsearch import AdaptiveSearchEngine
except ImportError as exc:  # pragma: no cover - dependencia opcional
    AdaptiveSearchEngine = None  # type: ignore
    _import_error = exc
else:
    _import_error = None

logger = logging.getLogger(__name__)

_engine: Optional[AdaptiveSearchEngine] = None
_engine_lock = Lock()


def _get_engine(
    quality_threshold: float, max_iterations: int, plateau_tolerance: int
) -> AdaptiveSearchEngine:
    """Inicializa el motor adaptativo una única vez."""
    if _import_error is not None:
        raise RuntimeError(
            "Falta la dependencia interna 'deepsearcher'. "
            "Revisa que el paquete esté instalado en el entorno."
        ) from _import_error

    global _engine
    with _engine_lock:
        if _engine is None:
            _engine = AdaptiveSearchEngine(
                quality_threshold=quality_threshold,
                max_iterations=max_iterations,
                plateau_tolerance=plateau_tolerance,
            )
        return _engine


def _run_coroutine_safely(coro: "asyncio.Future[Any]") -> Any:
    """
    Ejecuta una corrutina garantizando compatibilidad con entornos sin bucle
    de eventos (p. ej. herramientas LangChain síncronas).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    new_loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(new_loop)
        return new_loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        new_loop.close()


class DeepSearchNewsTool(BaseTool):
    """Herramienta que usa DeepSearch para encontrar noticias recientes con fuentes."""

    name: str = "deepsearch_news_oracle"
    description: str = (
        "Realiza búsquedas web iterativas usando el motor interno DeepSearch, "
        "combina múltiples fuentes y devuelve un resumen en español con URLs verificables. "
        "Úsala cuando necesites información periodística, hechos recientes o contexto "
        "web actualizado."
    )

    quality_threshold: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Calidad mínima objetivo (0-1) para detener la búsqueda.",
    )
    max_iterations: int = Field(
        default=12,
        gt=0,
        description="Número máximo de iteraciones de refinamiento.",
    )
    plateau_tolerance: int = Field(
        default=5,
        ge=1,
        description="Iteraciones sin mejora antes de detenerse por estancamiento.",
    )
    min_sources: int = Field(
        default=4,
        ge=1,
        description="Número mínimo deseado de fuentes antes de detener la búsqueda.",
    )
    max_sources: int = Field(
        default=8,
        gt=0,
        description="Número máximo de URLs que se citarán en la respuesta.",
    )

    def _run(self, query: str) -> str:
        query = (query or "").strip()
        if not query:
            return "La consulta está vacía. Proporciona una pregunta o tema concreto."

        try:
            engine = _get_engine(
                quality_threshold=self.quality_threshold,
                max_iterations=self.max_iterations,
                plateau_tolerance=self.plateau_tolerance,
            )
            # Asegurar que los parámetros dinámicos se actualicen aunque el motor exista.
            engine.quality_threshold = self.quality_threshold
            engine.max_iterations = self.max_iterations
            engine.plateau_tolerance = self.plateau_tolerance
            if hasattr(engine, "min_sources_threshold"):
                engine.min_sources_threshold = max(engine.min_sources_threshold, self.min_sources)

            async def _search() -> Dict[str, Any]:
                return await engine.search_with_feedback_loop(query)

            result: Dict[str, Any] = _run_coroutine_safely(_search())
        except Exception as exc:  # pragma: no cover - manejar en ejecución
            logger.exception("Error ejecutando DeepSearchNewsTool")
            return f"Error al ejecutar DeepSearch: {exc}"

        answer = (result.get("final_answer") or "").strip()
        sources = result.get("evidence_sources") or []

        if not answer:
            answer = (
                "No he podido obtener una síntesis fiable con la información disponible. "
                "Intenta reformular la pregunta o proporcionar más contexto."
            )

        formatted_sources = self._format_sources(sources)
        if formatted_sources:
            return f"{answer}\n\nFuentes:\n{formatted_sources}"

        return (
            f"{answer}\n\n(No se identificaron fuentes verificadas durante la búsqueda.)"
        )

    async def _arun(self, query: str) -> str:  # pragma: no cover - delega en sync
        return self._run(query)

    def _format_sources(self, sources: List[str]) -> str:
        unique_sources: List[str] = []
        seen = set()
        for url in sources:
            url = (url or "").strip()
            if not url or not url.startswith(("http://", "https://")):
                continue
            if url in seen:
                continue
            seen.add(url)
            unique_sources.append(url)
            if len(unique_sources) >= self.max_sources:
                break

        return "\n".join(f"- {url}" for url in unique_sources)


def create_deepsearch_tool() -> DeepSearchNewsTool:
    """Crea una instancia lista para usarse como herramienta LangChain."""
    return DeepSearchNewsTool()


__all__ = ["create_deepsearch_tool", "DeepSearchNewsTool"]
