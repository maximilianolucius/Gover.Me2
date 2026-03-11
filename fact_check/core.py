"""Core validation workflow for political claim fact-checking."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

try:
    from .tools import duckduckgo_search, rag_newspapers, tourism_stats
except ImportError:  # pragma: no cover - allow script execution
    import os
    import sys

    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from tools import duckduckgo_search, rag_newspapers, tourism_stats  # type: ignore


_NUMBER_RE = re.compile(r"(?:\d+[\.,]?\d*)(?:\s*(?:millones|millón|m|k))?", re.IGNORECASE)
_DATE_RE = re.compile(r"\b(19|20)\d{2}\b")
_ENTITY_RE = re.compile(r"[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*")
_TOURISM_KEYWORDS = {"turismo", "turista", "visitantes", "pernoctaciones", "ocupación hotelera"}


@dataclass
class Claim:
    """Normalized representation of an incoming claim."""

    texto: str
    categoria: str
    justificacion: str = ""


@dataclass
class ClaimAnalysis:
    """Extraction of salient entities and metadata for a claim."""

    numeric_values: List[float] = field(default_factory=list)
    primary_numeric_value: Optional[float] = None
    dates: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    claim_type: str = "factual"
    mentioned_sources: List[str] = field(default_factory=list)
    domain_tags: List[str] = field(default_factory=list)


@dataclass
class VerificationComponent:
    """Holds the outcome of validating a claim against a specific source family."""

    puntuacion: int
    detalles: Dict[str, Any]
    supporting_sources: int = 0


class ClaimValidator:
    """High-level orchestrator for multi-source claim validation."""

    def __init__(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Stage 1: Analysis helpers
    # ------------------------------------------------------------------
    def _analyze_claim(self, claim: Claim) -> ClaimAnalysis:
        text = claim.texto
        justification = claim.justificacion or ""

        numeric_values: List[float] = []
        for match in _NUMBER_RE.findall(text):
            value = self._normalize_number(match)
            if value is not None:
                numeric_values.append(value)

        dates = _DATE_RE.findall(text)
        entities = self._extract_entities(text)
        mentioned_sources = self._extract_sources_from_justification(justification)

        primary_numeric_value = self._select_primary_numeric_value(numeric_values)
        claim_type = self._determine_claim_type(numeric_values, dates, entities)
        domain_tags = self._determine_domain_tags(text, justification)

        return ClaimAnalysis(
            numeric_values=numeric_values,
            primary_numeric_value=primary_numeric_value,
            dates=dates,
            entities=entities,
            claim_type=claim_type,
            mentioned_sources=mentioned_sources,
            domain_tags=domain_tags,
        )

    @staticmethod
    def _normalize_number(raw: str) -> Optional[float]:
        raw_l = raw.lower().replace(",", ".")
        multiplier = 1.0
        if "mill" in raw_l:
            multiplier = 1_000_000.0
            raw_l = raw_l.replace("millones", "").replace("millón", "").strip()
        elif raw_l.endswith("m"):
            multiplier = 1_000_000.0
            raw_l = raw_l[:-1]
        elif raw_l.endswith("k"):
            multiplier = 1_000.0
            raw_l = raw_l[:-1]
        try:
            return float(raw_l) * multiplier
        except ValueError:
            return None

    @staticmethod
    def _extract_entities(text: str) -> List[str]:
        entities: List[str] = []
        for match in _ENTITY_RE.findall(text):
            cleaned = match.strip()
            if len(cleaned.split()) > 1 and cleaned.lower() not in {"el informe"}:
                entities.append(cleaned)
        return list(dict.fromkeys(entities))

    @staticmethod
    def _extract_sources_from_justification(justification: str) -> List[str]:
        keywords = ["según", "de acuerdo", "con base en", "informe", "estudio", "ministerio"]
        lower = justification.lower()
        found = []
        for keyword in keywords:
            if keyword in lower:
                found.append(keyword)
        return found

    @staticmethod
    def _determine_claim_type(
        numeric_values: Iterable[float],
        dates: Iterable[str],
        entities: Iterable[str],
    ) -> str:
        if numeric_values:
            return "numeric"
        if dates:
            return "temporal"
        if entities:
            return "factual"
        return "factual"

    @staticmethod
    def _determine_domain_tags(text: str, justification: str) -> List[str]:
        tokens = set(text.lower().split()) | set(justification.lower().split())
        tags = []
        if any(keyword in tokens for keyword in _TOURISM_KEYWORDS) or any(
            token.startswith("turist") or token.startswith("pernoct") for token in tokens
        ):
            tags.append("turismo")
        return tags

    @staticmethod
    def _select_primary_numeric_value(values: List[float]) -> Optional[float]:
        if not values:
            return None
        return max(values, key=lambda x: abs(x))

    @staticmethod
    def _detect_contradiction(summary: str) -> bool:
        if not summary:
            return False
        lowered = summary.lower()
        keywords = [
            "no se encontró evidencia",
            "no hay evidencia",
            "no se hallaron",
            "contradic",
            "desmentido",
            "no coincide",
        ]
        return any(keyword in lowered for keyword in keywords)

    # ------------------------------------------------------------------
    # Stage 2: Multi-source verification
    # ------------------------------------------------------------------
    def _verify_with_rag(self, claim: Claim, analysis: ClaimAnalysis) -> VerificationComponent:
        try:
            raw_result = rag_newspapers(claim.texto)
        except Exception as exc:  # pragma: no cover
            return VerificationComponent(
                puntuacion=0,
                detalles={
                    "error": f"Error consultando RAG de periódicos: {exc}",
                    "menciones_encontradas": [],
                    "fuentes_citadas": [],
                    "resumen": "",
                },
            )

        if isinstance(raw_result, dict):
            mentions_data = raw_result.get("mentions", [])
            summary = raw_result.get("summary", "")
        else:
            mentions_data = raw_result
            summary = ""

        score = 0
        supporting_sources = 0
        cited_sources: List[str] = []

        unique_sources = {item.get("fuente") for item in mentions_data if item.get("fuente")}
        count = len(unique_sources)

        contradictory = self._detect_contradiction(summary)
        if contradictory:
            score = -2
        elif count >= 2:
            score = 3
            supporting_sources = count
        elif count == 1:
            score = 2
            supporting_sources = 1
        elif mentions_data:
            score = 1
            supporting_sources = 1

        for item in mentions_data:
            fuente = item.get("fuente")
            if fuente:
                cited_sources.append(fuente)

        detalles = {
            "puntuacion": score,
            "menciones_encontradas": mentions_data,
            "fuentes_citadas": list(filter(None, cited_sources)),
            "resumen": summary,
        }

        return VerificationComponent(score, detalles, supporting_sources if score > 0 else 0)

    def _verify_with_duckduckgo(self, claim: Claim, analysis: ClaimAnalysis) -> VerificationComponent:
        try:
            results = duckduckgo_search(claim.texto)
        except Exception as exc:  # pragma: no cover
            return VerificationComponent(
                puntuacion=0,
                detalles={
                    "error": f"Error consultando DuckDuckGo: {exc}",
                    "resultados_relevantes": [],
                    "fuentes_oficiales": [],
                },
            )

        score = 0
        official_results = [r for r in results if r.get("type") == "official" and r.get("stance") != "contradictory"]
        contradictory = any(r.get("stance") == "contradictory" for r in results)

        if contradictory:
            score = -1
        elif official_results:
            score = 2
        elif len(results) >= 2:
            score = 1
        elif results:
            score = 1

        supporting_sources = 1 if score > 0 else 0

        detalles = {
            "puntuacion": score,
            "resultados_relevantes": results,
            "fuentes_oficiales": [r for r in official_results],
        }

        return VerificationComponent(score, detalles, supporting_sources)

    def _verify_with_tourism(self, claim: Claim, analysis: ClaimAnalysis) -> VerificationComponent:
        if "turismo" not in analysis.domain_tags:
            return VerificationComponent(0, {"puntuacion": 0, "datos_oficiales": {}, "coincidencia": "no_aplica"})

        try:
            stats = tourism_stats(claim.texto)
        except Exception as exc:  # pragma: no cover
            return VerificationComponent(
                puntuacion=0,
                detalles={
                    "error": f"Error consultando estadísticas de turismo: {exc}",
                    "datos_oficiales": {},
                    "coincidencia": "desconocida",
                },
            )

        if not stats:
            detalles = {
                "puntuacion": 0,
                "datos_oficiales": {},
                "coincidencia": "no_disponible",
            }
            return VerificationComponent(0, detalles)

        official_value = stats.get("value")
        coincidence = "desconocida"
        score = 0

        if official_value is None or analysis.primary_numeric_value is None:
            score = 0
            coincidence = "datos_insuficientes"
        else:
            claim_value = analysis.primary_numeric_value
            diff = abs(claim_value - official_value)
            if diff == 0:
                score = 3
                coincidence = "exacta"
            else:
                relative_diff = diff / max(official_value, 1)
                if relative_diff <= 0.1:
                    score = 2
                    coincidence = "aproximada"
                else:
                    score = -3
                    coincidence = "contradictoria"

        detalles = {
            "puntuacion": score,
            "datos_oficiales": stats,
            "coincidencia": coincidence,
            "raw_answer": stats.get("raw_answer", ""),
        }

        supporting_sources = 1 if score > 0 else 0

        return VerificationComponent(score, detalles, supporting_sources)

    # ------------------------------------------------------------------
    # Scoring aggregation
    # ------------------------------------------------------------------
    @staticmethod
    def _classify(total_score: int, supporting_sources: int) -> str:
        return "Verdadera" if total_score >= 5 and supporting_sources >= 2 else "Falsa"

    @staticmethod
    def _verdict(total_score: int, supporting_sources: int) -> str:
        return "Verdadera" if total_score >= 5 and supporting_sources >= 2 else "Falsa"

    def _summarize(self, claim: Claim, analysis: ClaimAnalysis, components: Dict[str, VerificationComponent], total_score: int) -> str:
        parts = [
            f"Afirmación evaluada como tipo '{analysis.claim_type}' con entidades detectadas: {', '.join(analysis.entities) or 'ninguna'}.",
            f"Puntuaciones parciales - RAG: {components['rag'].puntuacion}, DuckDuckGo: {components['web'].puntuacion}, Turismo: {components['tourism'].puntuacion}.",
        ]
        if analysis.primary_numeric_value is not None:
            parts.append(f"Valor numérico destacado: {analysis.primary_numeric_value:,.0f}.")
        if components["tourism"].detalles.get("coincidencia") in {"exacta", "aproximada", "contradictoria"}:
            parts.append(
                f"Comparación con datos oficiales: {components['tourism'].detalles.get('coincidencia')}"
            )
        parts.append(f"Puntuación total: {total_score}.")
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def validate(self, claims: Iterable[Claim]) -> List[Dict[str, Any]]:
        results = []
        for claim in claims:
            analysis = self._analyze_claim(claim)

            rag_component = self._verify_with_rag(claim, analysis)
            web_component = self._verify_with_duckduckgo(claim, analysis)
            tourism_component = self._verify_with_tourism(claim, analysis)

            components = {
                "rag": rag_component,
                "web": web_component,
                "tourism": tourism_component,
            }

            total_score = sum(component.puntuacion for component in components.values())
            supporting_sources = sum(component.supporting_sources for component in components.values())
            status = self._classify(total_score, supporting_sources)
            summary = self._summarize(claim, analysis, components, total_score)

            result = {
                "texto_original": claim.texto,
                "categoria_original": claim.categoria,
                "puntuacion_total": total_score,
                "estado_validacion": status,
                "veredicto": status,
                "detalles_verificacion": {
                    "rag_periodicos": rag_component.detalles,
                    "busqueda_web": web_component.detalles,
                    "estadisticas_turismo": tourism_component.detalles,
                },
                "resumen_evidencia": summary,
            }

            results.append(result)

        return results


def validate_claims(claims_list: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convenience wrapper that accepts raw dictionaries."""
    claims = [
        Claim(
            texto=item.get("texto") or item.get("texto_original") or "",
            categoria=item.get("categoria") or item.get("categoria_original") or "",
            justificacion=item.get("justificacion") or "",
        )
        for item in claims_list
    ]
    validator = ClaimValidator()
    return validator.validate(claims)


if __name__ == "__main__":
    sample_input = {
        "afirmaciones": [
            {
                "texto": "El informe indica que en 2023 llegaron 2.5 millones de turistas a Andalucía",
                "categoria": "AFIRMACIÓN",
                "justificacion": "Dato numérico respaldado por informes oficiales de turismo.",
            }
        ],
        "ambiguas": [
            {
                "texto": "pero algunos analistas dicen que la tendencia podría cambiar pronto.",
                "categoria": "AMBIGUA",
                "justificacion": "Declaración vaga sin datos verificables.",
            }
        ],
    }

    results = validate_claims(sample_input["afirmaciones"])
    from pprint import pprint

    print("Resultados de validación para afirmaciones concretas:\n")
    pprint(results)
