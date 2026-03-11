#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Herramienta LangChain para consultar Nexus (datos de turismo Andalucía 2022-2025)."""

from __future__ import annotations

import logging
from typing import Optional, List
from threading import Lock

from langchain_core.tools import BaseTool
from pydantic import Field

try:
    from pdfkg.nexus_query import NexusQueryEngine
except Exception as exc:  # pragma: no cover - fallback to clear error at runtime
    NexusQueryEngine = None  # type: ignore
    _import_error = exc
else:
    _import_error = None

logger = logging.getLogger(__name__)

_engine: Optional[NexusQueryEngine] = None
_engine_lock = Lock()


def _get_engine() -> NexusQueryEngine:
    """Inicializa NexusQueryEngine una única vez."""
    global _engine
    if _import_error is not None:
        if isinstance(_import_error, ModuleNotFoundError) and _import_error.name == "google.generativeai":
            raise RuntimeError(
                "Falta la dependencia opcional 'google-generativeai'. "
                "Instala con `pip install google-generativeai>=0.3.0` y configura GEMINI_API_KEY."
            ) from _import_error
        raise RuntimeError(
            "No se pudo importar NexusQueryEngine: %s" % (_import_error,)
        ) from _import_error

    with _engine_lock:
        if _engine is None:
            _engine = NexusQueryEngine()
        return _engine


class NexusTourismTool(BaseTool):
    """Herramienta para preguntas sobre datos turísticos oficiales de Andalucía."""

    name: str = "nexus_tourism_oracle"
    description: str = (
        "Consulta el sistema Nexus de turismo de Andalucía (datos oficiales 2022-2025). "
        "Úsala para responder preguntas con métricas concretas, comparaciones por provincia, "
        "y cifras de visitantes o gasto turístico. Devuelve respuestas citando valores clave y fuentes."
    )
    save_history: bool = Field(default=True, description="Si guardar historial en Nexus DB")

    def _run(self, query: str) -> str:
        try:
            engine = _get_engine()
            result = engine.answer_question(query, save_history=self.save_history)
        except Exception as exc:  # pragma: no cover - manejado en tiempo de ejecución
            logger.exception("Error ejecutando NexusTourismTool")
            return f"Error al consultar datos turísticos: {exc}"

        answer = result.get("answer", "").strip()
        if not answer:
            answer = "No se obtuvo respuesta del sistema Nexus."

        details: List[str] = []
        sources = [src for src in result.get("sources", []) if src]
        if sources:
            top_sources = ", ".join(sources[:3])
            details.append(f"Fuentes: {top_sources}")
        duration = result.get("duration_seconds")
        if duration is not None:
            details.append(f"Tiempo de consulta: {duration:.2f}s")
        if result.get("num_results") is not None:
            details.append(f"Registros considerados: {result['num_results']}")

        if details:
            answer = f"{answer}\n\n{'; '.join(details)}"

        return answer

    async def _arun(self, query: str) -> str:  # pragma: no cover - async delega a sync
        return self._run(query)


def create_nexus_tool(save_history: bool = True) -> NexusTourismTool:
    """Retorna instancia lista para usarse en LangChain."""
    return NexusTourismTool(save_history=save_history)


__all__ = ["create_nexus_tool", "NexusTourismTool"]
