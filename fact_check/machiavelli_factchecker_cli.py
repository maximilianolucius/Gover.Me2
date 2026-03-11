"""Wrapper CLI helpers to fact-check text using the core validator pipeline.

This module exposes ``classify_paragraph`` with the same signature and output
shape as ``machiavelli_factcheck_cli.classify_paragraph`` but relies on the
``fact_check.core`` validator and the ``llmparser`` helper to extract claims.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fact_check.core import validate_claims
from ws_utils.vertex_ai_google_util import llmparser


# ---------------------------------------------------------------------------
# Global cache (module-level so it survives across invocations)
# ---------------------------------------------------------------------------

try:
    from pathlib import Path
except ImportError:  # pragma: no cover - Python < 3.4 (not expected)
    Path = None  # type: ignore

_CLAIM_CACHE_LOCK = Lock()

_CACHE_DB_PATH: Optional["Path"] = None
if 'Path' in globals() and Path is not None:  # pragma: no branch
    hostname = socket.gethostname()
    default_cache_path = Path(__file__).with_name(f"machiavelli_factchecker_cache_{hostname}.db")
    cache_env = os.getenv("MACHIAVELLI_FACTCHECK_CACHE")
    if cache_env:
        _CACHE_DB_PATH = Path(cache_env)
    else:
        _CACHE_DB_PATH = default_cache_path


def _init_cache_db() -> None:
    if not _CACHE_DB_PATH:
        return
    try:
        _CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(_CACHE_DB_PATH) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    claim_text TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()
    except Exception:
        pass


def _load_cache_from_db() -> Dict[str, Dict[str, Any]]:
    if not _CACHE_DB_PATH or not _CACHE_DB_PATH.exists():
        return {}
    try:
        with sqlite3.connect(_CACHE_DB_PATH) as conn:
            cursor = conn.execute("SELECT claim_text, payload FROM cache")
            rows = cursor.fetchall()
        cache: Dict[str, Dict[str, Any]] = {}
        for claim_text, payload in rows:
            try:
                cache[str(claim_text)] = json.loads(payload)
            except Exception:
                continue
        return cache
    except Exception:
        return {}


def _persist_claim_to_db(claim_text: str, result: Dict[str, Any]) -> None:
    if not _CACHE_DB_PATH:
        return
    try:
        payload = json.dumps(result, ensure_ascii=False)
        with sqlite3.connect(_CACHE_DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (claim_text, payload, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (claim_text, payload),
            )
            conn.commit()
    except Exception:
        pass


_init_cache_db()
_CLAIM_CACHE: Dict[str, Dict[str, Any]] = _load_cache_from_db()


# ---------------------------------------------------------------------------
# Optional Ray integration
# ---------------------------------------------------------------------------

try:  # pragma: no cover - optional dependency
    import ray  # type: ignore

    _RAY_AVAILABLE = True
except Exception:  # pragma: no cover - Ray not installed
    ray = None  # type: ignore
    _RAY_AVAILABLE = False


if _RAY_AVAILABLE:

    @ray.remote
    def _validate_claim_remote(idx: int, claim_payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Ray task that delegates to ``validate_claims`` for a single claim."""

        result = validate_claims([claim_payload])[0]
        return idx, result


def _default_response(user_text: str, error: str = "") -> Dict[str, Any]:
    """Return an empty response envelope mirroring the legacy CLI format."""

    length = len(user_text)
    return {
        "code": 0,
        "data": {
            "ideas_de_fuerza": {
                "data": [],
                "score_line": [],
            },
            "fuentes": [],
            "discurso": {
                "text": user_text,
                "color": [0.0] * length,
                "claim_mask": [0] * length,
            },
            "Titulares": {
                "Afirmaciones Criticas": [],
                "Afirmaciones Ambiguas": [],
            },
        },
        "error": error,
        "is_done": True,
    }


def _normalize_claim_entry(item: Any) -> Optional[Dict[str, Any]]:
    """Normalize the structure returned by ``llmparser`` into core payloads."""

    if isinstance(item, str):
        texto = item.strip()
        if not texto:
            return None
        return {
            "texto": texto,
            "categoria": "AFIRMACIÓN",
            "justificacion": "",
        }

    if isinstance(item, dict):
        texto = (item.get("texto") or item.get("claim") or item.get("text") or "").strip()
        if not texto:
            return None
        categoria = item.get("categoria") or item.get("categoria_original") or "AFIRMACIÓN"
        justificacion = item.get("justificacion") or item.get("rationale") or item.get("explicacion") or ""
        return {
            "texto": texto,
            "categoria": categoria,
            "justificacion": justificacion,
        }

    return None


def _extract_claims(user_text: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Use the llmparser helper to extract factual and ambiguous claims."""

    parser_kwargs = {
        "provider": os.getenv("LLMPARSER_PROVIDER", "gemini"),
    }

    parsed = llmparser(user_text, **parser_kwargs)
    afirmaciones_raw = parsed.get("afirmaciones") or []
    ambiguas_raw = parsed.get("ambiguas") or []

    afirmaciones: List[Dict[str, Any]] = []
    for item in afirmaciones_raw:
        normalized = _normalize_claim_entry(item)
        if normalized:
            afirmaciones.append(normalized)

    ambiguas: List[Dict[str, Any]] = []
    for item in ambiguas_raw:
        normalized = _normalize_claim_entry(item)
        if normalized:
            ambiguas.append(normalized)

    if not afirmaciones:
        # Fallback: treat entire paragraph as a single claim
        fallback = _normalize_claim_entry(user_text)
        if fallback:
            afirmaciones.append(fallback)

    return afirmaciones, ambiguas


def _claim_positions(user_text: str, claims: Iterable[str]) -> List[Tuple[str, int, int]]:
    """Best-effort mapping from claim text to character positions."""

    lower_text = user_text.lower()
    current_index = 0
    positions: List[Tuple[str, int, int]] = []

    for claim in claims:
        normalized_claim = claim.strip()
        if not normalized_claim:
            positions.append((claim, 0, 0))
            continue

        claim_lower = normalized_claim.lower()
        found_at = lower_text.find(claim_lower, current_index)
        if found_at == -1:
            found_at = lower_text.find(claim_lower)
        if found_at == -1:
            positions.append((claim, 0, 0))
            continue

        start = found_at
        end = found_at + len(normalized_claim)
        positions.append((claim, start, end))
        current_index = end

    return positions


def _fetch_from_cache(claim_text: str) -> Optional[Dict[str, Any]]:
    with _CLAIM_CACHE_LOCK:
        return _CLAIM_CACHE.get(claim_text)


def _store_in_cache(claim_text: str, result: Dict[str, Any]) -> None:
    with _CLAIM_CACHE_LOCK:
        _CLAIM_CACHE[claim_text] = result
        _persist_claim_to_db(claim_text, result)


def _parallel_validate(claim_payloads: List[Tuple[int, Dict[str, Any]]]) -> Dict[int, Dict[str, Any]]:
    """Validate claims in parallel using Ray when available, otherwise threads."""

    if not claim_payloads:
        return {}

    results: Dict[int, Dict[str, Any]] = {}

    if _RAY_AVAILABLE:
        if not ray.is_initialized():  # pragma: no cover - runtime guard
            ray.init(ignore_reinit_error=True, log_to_driver=False)

        futures = [_validate_claim_remote.remote(idx, payload) for idx, payload in claim_payloads]
        remote_results = ray.get(futures)
        for idx, data in remote_results:
            results[idx] = data
        return results

    # ThreadPool fallback keeps compatibility when Ray is not installed
    with ThreadPoolExecutor(max_workers=min(4, len(claim_payloads))) as executor:
        future_map = {
            executor.submit(validate_claims, [payload]): idx
            for idx, payload in claim_payloads
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            validated = future.result()[0]
            results[idx] = validated

    return results


def _summaries_from_result(result: Dict[str, Any]) -> Tuple[str, str, str]:
    """Derive textual summaries for legacy fields from the core validator output."""

    estado = (result.get("estado_validacion") or "").lower()
    resumen = result.get("resumen_evidencia") or ""

    if estado == "verdadera":
        incoherencia = "No se detectan incoherencias con las fuentes consultadas."
        verificado = "Las evidencias respaldan la afirmación."
        sugerencia = "Puedes destacar este punto con respaldo documental."
    else:
        incoherencia = "Las fuentes consultadas no respaldan el enunciado."
        verificado = "El sistema considera la afirmación como falsa o con evidencia insuficiente."
        sugerencia = "Recomienda revisar y solicitar fuentes adicionales sobre este punto."

    if resumen:
        incoherencia = resumen

    return incoherencia, verificado, sugerencia


def _collect_referencias(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert detailed evidence into the referencias structure expected downstream."""

    detalles = result.get("detalles_verificacion", {})
    referencias: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _add_reference(fuente: str, title: str, url: str) -> None:
        key = url or f"{fuente}|{title}"
        if key in seen:
            return
        seen.add(key)
        referencias.append(
            {
                "fuente": fuente or "Desconocida",
                "id": len(referencias) + 1,
                "title": title[:100] + "..." if len(title) > 100 else title,
                "url": url,
            }
        )

    rag_block = detalles.get("rag_periodicos", {})
    for entry in rag_block.get("menciones_encontradas", []) or []:
        fuente = str(entry.get("fuente") or entry.get("source") or entry.get("medio") or "")
        title = str(entry.get("titulo") or entry.get("title") or entry.get("extracto") or entry.get("snippet") or "")
        url = str(entry.get("url") or entry.get("enlace") or "")
        if title or url:
            _add_reference(fuente, title, url)

    web_block = detalles.get("busqueda_web", {})
    for entry in web_block.get("resultados_relevantes", []) or []:
        fuente = str(entry.get("source") or entry.get("fuente") or "")
        title = str(entry.get("title") or entry.get("snippet") or "")
        url = str(entry.get("url") or "")
        if title or url:
            _add_reference(fuente, title, url)

    tourism_block = detalles.get("estadisticas_turismo", {})
    datos_oficiales = tourism_block.get("datos_oficiales") or {}
    if datos_oficiales:
        fuente = str(datos_oficiales.get("fuente") or "Datos oficiales de turismo")
        title = str(
            datos_oficiales.get("raw_answer")
            or datos_oficiales.get("descripcion")
            or "Datos de turismo"
        )
        url = str(datos_oficiales.get("url") or "")
        _add_reference(fuente, title, url)

    return referencias


def _score_from_result(result: Dict[str, Any]) -> float:
    """Map the core total score to the legacy [-1, 1] floating scale."""

    estado = (result.get("estado_validacion") or "").lower()
    total = float(result.get("puntuacion_total", 0))

    if estado == "verdadera":
        return min(1.0, max(0.5, total / 10.0 if total else 1.0))
    if estado == "falsa":
        return max(-1.0, min(-0.5, -abs(total) / 10.0 if total else -1.0))
    return 0.0


def _claim_results_for_discurso(score_line: List[float]) -> List[Dict[str, Any]]:
    """Create claim result skeletons for discourse rendering."""

    results: List[Dict[str, Any]] = []
    for value in score_line:
        if value > 0.2:
            classification = "true"
        elif value < -0.2:
            classification = "fake"
        else:
            classification = "insufficient"
        results.append({"score": value, "classification": classification})
    return results


def _generate_discurso_arrays(
    user_text: str,
    claim_results: List[Dict[str, Any]],
    claims_with_positions: List[Tuple[str, int, int]],
) -> Dict[str, Any]:
    """Port of the discourse heatmap computation from the legacy CLI."""

    length = len(user_text)
    color = [0.0] * length
    claim_mask = [0] * length

    for idx, (claim_text, start, end) in enumerate(claims_with_positions):
        if idx >= len(claim_results):
            continue
        start = max(0, min(start, length))
        end = max(start, min(end, length))
        result = claim_results[idx]
        score = float(result.get("score", 0.0))
        classification = result.get("classification", "insufficient")
        intensity = min(1.0, abs(score))
        if classification == "true":
            mask_value = 1
        elif classification == "fake":
            mask_value = -1
        else:
            mask_value = 0
        for pos in range(start, end):
            if 0 <= pos < length:
                color[pos] = intensity
                claim_mask[pos] = mask_value

    return {
        "text": user_text,
        "color": color,
        "claim_mask": claim_mask,
    }


def classify_paragraph(
    chatbot: Any,
    user_text: str,
    top_k: int,
    max_context_chars: int,
    debug: bool = False,
) -> Dict[str, Any]:
    """Classify a paragraph, mirroring the legacy output contract."""

    del chatbot  # Parameter kept for compatibility but unused in this implementation.
    del top_k, max_context_chars, debug

    try:
        afirmaciones, ambiguas = _extract_claims(user_text)
    except Exception as exc:
        return _default_response(user_text, error=f"Error al extraer afirmaciones: {exc}")

    claim_texts = [claim["texto"] for claim in afirmaciones]
    claim_positions = _claim_positions(user_text, claim_texts)

    ordered_results: Dict[int, Dict[str, Any]] = {}
    missing_payloads: List[Tuple[int, Dict[str, Any]]] = []

    for idx, claim in enumerate(afirmaciones):
        cached = _fetch_from_cache(claim["texto"])
        if cached is not None:
            ordered_results[idx] = cached
            continue
        missing_payloads.append((idx, claim))

    parallel_results = _parallel_validate(missing_payloads)
    for idx, result in parallel_results.items():
        ordered_results[idx] = result
        _store_in_cache(afirmaciones[idx]["texto"], result)

    # Ensure we have results for every claim (cached + newly computed)
    results_in_order: List[Dict[str, Any]] = []
    for idx in range(len(afirmaciones)):
        result = ordered_results.get(idx)
        if result is None:
            # Should not happen, but keep structure consistent
            result = validate_claims([afirmaciones[idx]])[0]
            _store_in_cache(afirmaciones[idx]["texto"], result)
        results_in_order.append(result)

    ideas_de_fuerza_data: List[Dict[str, Any]] = []
    score_line: List[float] = []
    all_fuentes: Dict[str, Dict[str, Any]] = {}

    for claim_payload, result in zip(afirmaciones, results_in_order):
        incoherencia, verificado, sugerencia = _summaries_from_result(result)
        referencias = _collect_referencias(result)
        for ref in referencias:
            key = ref.get("url") or f"{ref.get('fuente')}|{ref.get('title')}"
            if key and key not in all_fuentes:
                all_fuentes[key] = {k: v for k, v in ref.items() if k != "id"}
        ideas_de_fuerza_data.append(
            {
                "claim": claim_payload["texto"],
                "incoherencia_detectada": incoherencia,
                "resultado_verificado": verificado,
                "respuesta_sugerida": sugerencia,
                "referencias": referencias,
            }
        )
        score_line.append(_score_from_result(result))

    claim_results = _claim_results_for_discurso(score_line)
    discurso = _generate_discurso_arrays(user_text, claim_results, claim_positions)

    titulares_criticas: List[Dict[str, str]] = []
    titulares_ambiguas: List[Dict[str, str]] = []

    for claim_payload, result, score in zip(afirmaciones, results_in_order, score_line):
        titulo = claim_payload["texto"][:50] + "..." if len(claim_payload["texto"]) > 50 else claim_payload["texto"]
        estado = (result.get("estado_validacion") or "").lower()
        if estado == "falsa" or score < 0:
            titulares_criticas.append({
                "Titulo": titulo,
                "respuesta_sugerida": "Revisa esta afirmación: el sistema detectó inconsistencias.",
            })
        elif 0 <= score <= 0.2 or estado not in {"verdadera", "falsa"}:
            titulares_ambiguas.append({
                "Titulo": titulo,
                "respuesta_sugerida": "Se requiere evidencia adicional para confirmar esta afirmación.",
            })

    for ambiguous in ambiguas:
        titulo = ambiguous["texto"][:50] + "..." if len(ambiguous["texto"]) > 50 else ambiguous["texto"]
        titulares_ambiguas.append({
            "Titulo": titulo,
            "respuesta_sugerida": "Texto ambiguo identificado por el analizador; considera clarificarlo.",
        })

    response = {
        "code": 0,
        "data": {
            "ideas_de_fuerza": {
                "data": ideas_de_fuerza_data,
                "score_line": score_line,
            },
            "fuentes": list(all_fuentes.values()),
            "discurso": discurso,
            "Titulares": {
                "Afirmaciones Criticas": titulares_criticas,
                "Afirmaciones Ambiguas": titulares_ambiguas,
            },
        },
        "error": "",
        "is_done": True,
    }

    return response


__all__ = ["classify_paragraph"]
