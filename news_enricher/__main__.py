#!/usr/bin/env python3
"""
News Enricher - Enrich JSON news files with NLP analysis using vLLM.

Usage:
    python news_enricher.py --root ./noticias --out ./noticias_enriched --workers 8

Environment variables:
    VLLM_API_KEY: API key for vLLM (default: sk-local-elysia-noop)
    VLLM_BASE_URL: Base URL for vLLM API (default: http://172.24.250.17:8000/v1)
    VLLM_MODEL: Model name (default: Qwen3-8B-AWQ)
"""
import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# Opcional: generar gráficos sin necesidad de display
try:
    import matplotlib  # type: ignore

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt  # type: ignore
    import matplotlib.patheffects as path_effects  # type: ignore
except Exception:  # pragma: no cover - entornos sin matplotlib
    matplotlib = None  # type: ignore
    plt = None  # type: ignore
    path_effects = None  # type: ignore

from .media_map import get_media_info, extract_domain, is_spanish_domain
from .nlp_client import NLPClient
from .axes_catalog import AXES_BY_TOPIC
from .data_validators import (
    validate_url,
    is_truncated_url,
    repair_url,
    is_non_article_content,
    detect_paywall_overlay,
    clean_paywall_content,
    normalize_time_field,
    infer_timezone_from_offset
)
from .schema_utils import (
    create_stable_id,
    normalize_datetime_to_utc,
    count_words,
    calculate_reading_time,
    normalize_title,
    extract_time_components,
    get_template,
    deep_merge
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


MEDIA_AFFINITY_SOCIAL_JSON = Path("./news_enricher/output/media_affinity_socialnet_scores.json")
SCATTER_IMAGE_OUTPUT_DIR = Path("./api_images")
SOCIAL_SCATTER_SUFFIX = "_social_net"


def _clamp_01(value: float) -> float:
    """Clamp value to [0, 1]."""
    return max(0.0, min(1.0, value))


def _reach_to_rgb(reach: float) -> Tuple[float, float, float]:
    """
    Map reach (0-100) to a gradient between a soft orange and deep red.
    Returns an RGB tuple with components in [0, 1].
    """
    reach_norm = _clamp_01(reach / 100.0)
    start = (1.0, 0.650, 0.0)   # anaranjado
    end = (0.835, 0.0, 0.0)     # rojo intenso
    return tuple(
        s + (e - s) * reach_norm
        for s, e in zip(start, end)
    )


def _annotate_account(ax, name: str, x: float, y: float) -> None:
    """Place account label near the point with gentle offset and outline."""
    dx = 6 if x <= 88 else -6
    dy = 6 if y <= 88 else -6
    ha = "left" if dx > 0 else "right"
    va = "bottom" if dy > 0 else "top"

    if plt is None:
        return

    text = ax.annotate(
        name,
        (x, y),
        xytext=(dx, dy),
        textcoords="offset points",
        fontsize=7,
        color="#1f1f1f",
        ha=ha,
        va=va,
        zorder=4,
    )

    if path_effects is not None:
        text.set_path_effects([
            path_effects.Stroke(linewidth=2.4, foreground="white"),
            path_effects.Normal(),
        ])


def generate_social_network_scatter_images(json_path: Path, output_dir: Path) -> None:
    """
    Generate scatter plots for social network affinity using media_affinity_scores.json.
    Each account is rendered as an orange→red marker with its handle nearby.
    """
    if plt is None:
        logger.warning("matplotlib no está disponible; se omite la generación de gráficos social_net.")
        return

    if not json_path.exists():
        logger.warning("No se encontró %s, se omite el gráfico social_net.", json_path)
        return

    try:
        with json_path.open("r", encoding="utf-8") as handle:
            affinity_data = json.load(handle)
    except json.JSONDecodeError as exc:
        logger.error("No se pudo parsear %s (%s).", json_path, exc)
        return

    if not isinstance(affinity_data, dict):
        logger.warning("Formato inesperado en %s; se esperaba un objeto JSON con periodos.", json_path)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    for window, entries in affinity_data.items():
        if not isinstance(entries, dict) or not entries:
            continue

        fig = plt.figure(figsize=(5.5, 4.0), dpi=180)
        fig.patch.set_alpha(0)
        ax = fig.add_subplot(111)
        ax.set_facecolor("none")
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.set_xlabel("Afinidad argumental")
        ax.set_ylabel("Alcance")
        ax.grid(True, linewidth=0.4, alpha=0.5)

        points = []
        for name, metrics in entries.items():
            if not isinstance(metrics, dict):
                continue
            affinity = max(0.0, min(100.0, float(metrics.get("affinity", 0) or 0)))
            reach = max(0.0, min(100.0, float(metrics.get("reach", 0) or 0)))
            points.append((name, affinity, reach))

        if not points:
            plt.close(fig)
            continue

        # Ordenar por reach para que los puntos pequeños no queden totalmente cubiertos
        points.sort(key=lambda item: item[2])

        xs = [p[1] for p in points]
        ys = [p[2] for p in points]
        colors = [_reach_to_rgb(p[2]) for p in points]

        ax.scatter(
            xs,
            ys,
            s=120,
            c=colors,
            edgecolors="#6d1e03",
            linewidths=0.9,
            alpha=0.93,
            zorder=3,
        )

        for name, x, y in points:
            _annotate_account(ax, name, x, y)

        plt.tight_layout()

        filename = output_dir / f"media_affinity_{window}{SOCIAL_SCATTER_SUFFIX}.png"
        fig.savefig(filename, format="png", transparent=True)
        plt.close(fig)
        logger.info("Generado gráfico social net: %s", filename)


class NewsEnricher:
    """Enrich news JSON files with NLP analysis."""

    def __init__(
        self,
        nlp_client: NLPClient,
        output_dir: Path,
        overwrite: bool = False,
        dry_run: bool = False,
        json_file_constant: bool = True
    ):
        self.nlp_client = nlp_client
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.json_file_constant = json_file_constant

        # Statistics
        self.stats = {
            'total': 0,
            'success': 0,
            'warnings': 0,
            'errors': 0
        }

        # CSV report
        self.report_rows = []

    def _read_json_file(self, file_path: Path) -> Optional[Dict]:
        """Read JSON file (supports .json and .json.gz)."""
        try:
            if file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None

    def _write_json_file(self, file_path: Path, data: Dict):
        """Write JSON file (compressed .json.gz)."""
        if self.json_file_constant:
            logger.debug(f"json_file_constant enabled - skipping write for {file_path}")
            return

        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Always write as gzip compressed
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _get_output_path(self, input_path: Path, root_dir: Path) -> Path:
        """
        Get output path with compression.
        - If input is .json -> output is .json.gz (overwrites original .json)
        - If input is .json.gz -> output is .json.gz (overwrites)
        """
        # If output_dir == root_dir, we're overwriting
        if self.output_dir == root_dir:
            # Overwrite mode: ensure output is .json.gz
            if input_path.suffix == '.gz':
                # Already .json.gz -> keep same path
                return input_path
            elif input_path.suffix == '.json':
                # .json -> .json.gz
                return input_path.with_suffix('.json.gz')
            else:
                # Unknown extension -> add .json.gz
                return input_path.with_suffix('.json.gz')
        else:
            # Separate output dir mode (legacy)
            relative = input_path.relative_to(root_dir)

            # Ensure output is .json.gz
            if relative.suffix == '.gz':
                # Already .json.gz
                pass
            elif relative.suffix == '.json':
                # .json -> .json.gz
                relative = relative.with_suffix('.json.gz')
            else:
                # Unknown -> .json.gz
                relative = relative.with_suffix('.json.gz')

            return self.output_dir / relative

    def _extract_original_fields(self, data: Dict) -> Tuple[str, str, str, str]:
        """
        Extract original fields from news data.
        Returns: (url, title, body, date)
        """
        # Common field names for URL
        url = (
            data.get('url') or
            data.get('url_original') or
            data.get('link') or
            data.get('source', {}).get('url') or
            ""
        )

        # Common field names for title
        title = (
            data.get('title') or
            data.get('headline') or
            data.get('titulo') or
            data.get('text', {}).get('title') or
            ""
        )

        # Common field names for body
        body = (
            data.get('contenido_completo') or  # Spanish: full content
            data.get('body') or
            data.get('content') or
            data.get('text') or
            data.get('descripcion') or
            data.get('text', {}).get('body') or
            data.get('text', {}).get('body_plain') or
            ""
        )

        # Common field names for date
        date = (
            data.get('published') or
            data.get('date') or
            data.get('fecha') or
            data.get('published_date') or
            data.get('time', {}).get('published_utc') or
            ""
        )

        return url, title, body, date

    def _enrich_document(self, data: Dict) -> Tuple[Dict, List[str], int, float]:
        """
        Enrich a single document.

        Returns:
            (enriched_data, warnings, tokens_used, latency_ms)
        """
        warnings = []
        total_tokens = 0
        total_latency = 0.0

        # Get template and merge
        template = get_template()
        enriched = deep_merge(data, template)

        # Extract original fields
        url, title, body, date = self._extract_original_fields(data)

        # Initialize audit tracking
        audit_errors = []
        audit_warnings = []
        audit_fixed = False

        # 0. VALIDATION: URL
        url_valid, url_error = validate_url(url)
        if not url_valid:
            audit_errors.append(f"url_invalid:{url_error}")
            audit_fixed = True

            # Attempt repair if truncated
            if url_error in ['truncated_year', 'invalid_format']:
                domain_temp = extract_domain(url) if url else None
                repaired = repair_url(url, domain_temp, title)
                if repaired:
                    enriched['url_original_repaired'] = repaired
                    audit_fixed = True
                    warnings.append(f"URL repaired: {url} -> {repaired}")

        # 0b. VALIDATION: Paywall detection and cleaning
        has_paywall, paywall_blocks = detect_paywall_overlay(body)
        if has_paywall:
            body = clean_paywall_content(body)
            enriched['source']['paywall'] = True
            audit_warnings.append("paywall_overlay_stripped")
            audit_fixed = True
            warnings.append(f"Paywall blocks removed: {len(paywall_blocks)}")

        # 0c. VALIDATION: Non-article content detection
        word_count_temp = count_words(body)
        is_non_article, non_article_reason = is_non_article_content(body, url, word_count_temp)
        if is_non_article:
            enriched['es_noticia_valida'] = False
            audit_errors.append(f"non_article_index:{non_article_reason}")
            enriched['audit']['excluded_reason'] = non_article_reason
            warnings.append(f"Non-article detected: {non_article_reason}")

        # 1. Generate stable ID
        # Use repaired URL if available, otherwise use domain fallback
        id_url = enriched.get('url_original_repaired') if not url_valid else url
        if not id_url or not url_valid:
            # Fallback to domain-based ID
            domain_temp = extract_domain(url) if url else "unknown"
            enriched['id'] = create_stable_id(domain_temp, title, date)
            enriched['audit']['id_confidence'] = 'low'
            audit_warnings.append("id_uses_domain_fallback")
        else:
            enriched['id'] = create_stable_id(id_url, title, date)

        # 2. Source fields
        domain = extract_domain(url)
        enriched['source']['domain'] = domain

        media_name, media_home = get_media_info(domain, url)
        enriched['source']['media']['name'] = media_name
        enriched['source']['media']['home_url'] = media_home

        # 3. Time fields
        published_utc = normalize_datetime_to_utc(date)
        if not published_utc:
            if date:
                warnings.append(f"Failed to normalize date: {date}")
                audit_warnings.append("no_publish_time")
            # Set all time fields to None (not "" or 0)
            enriched['time']['published_utc'] = None
            enriched['time']['year'] = None
            enriched['time']['month'] = None
            enriched['time']['day'] = None
            enriched['time']['iso_week'] = None
        else:
            enriched['time']['published_utc'] = published_utc
            time_components = extract_time_components(published_utc)
            enriched['time'].update(time_components)

        enriched['time']['extracted_utc'] = datetime.now(timezone.utc).isoformat()

        # Set timezone for Spanish domains
        if is_spanish_domain(domain):
            enriched['time']['published_tz'] = 'Europe/Madrid'
        else:
            enriched['time']['published_tz'] = None

        # 3b. Geo fields
        if is_spanish_domain(domain):
            enriched['geo']['country'] = 'ES'

        # 4. Text fields
        enriched['text']['title_norm'] = normalize_title(title)
        enriched['text']['body_plain'] = body.strip()

        word_count = count_words(body)
        enriched['text']['word_count'] = word_count
        enriched['text']['reading_time_min'] = calculate_reading_time(word_count)

        # 5. NLP Classification (skip if non-article)
        if is_non_article:
            # Neutralize classification for non-articles
            enriched['classify']['primary_topic'] = ''
            enriched['classify']['subtopics'] = []
            enriched['classify']['stance_by_party'] = {
                'pp': 0.0, 'psoe': 0.0, 'vox': 0.0,
                'bng': 0.0, 'sumar': 0.0, 'podemos': 0.0
            }
            enriched['classify']['argument_affinity_index'] = 0.0
        else:
            classify_result, tokens, latency = self.nlp_client.classify(title, body)
            total_tokens += tokens
            total_latency += latency

            if classify_result:
                # Map classification fields
                enriched['classify']['primary_topic'] = classify_result.get('primary_topic', '')
                enriched['classify']['subtopics'] = classify_result.get('subtopics', [])
                enriched['classify']['sentiment_label'] = classify_result.get('sentiment_label', 'neutral')
                enriched['classify']['sentiment_score'] = classify_result.get('sentiment_score', 0.0)

                stance = classify_result.get('stance_by_party', {})
                enriched['classify']['stance_by_party'].update(stance)

                enriched['classify']['argument_affinity_index'] = classify_result.get('argument_affinity_index', 0.0)
            else:
                warnings.append("Classification failed")

        # 6. NLP Summary (skip if non-article)
        if not is_non_article:
            summary_result, tokens, latency = self.nlp_client.summarize(title, body)
            total_tokens += tokens
            total_latency += latency

            if summary_result:
                enriched['nlp']['model'] = self.nlp_client.model
                enriched['nlp']['summary_abstractive'] = summary_result.get('summary_abstractive', '')
                enriched['nlp']['bullets_extractive'] = summary_result.get('bullets_extractive', [])
                enriched['nlp']['keywords'] = summary_result.get('keywords', [])
            else:
                warnings.append("Summarization failed")

        # 6b. NLP Entities Extraction (skip if non-article)
        parties_present = []
        if not is_non_article:
            entities_result, tokens, latency = self.nlp_client.extract_entities(title, body)
            total_tokens += tokens
            total_latency += latency

            if entities_result:
                enriched['entities']['persons'] = entities_result.get('persons', [])
                enriched['entities']['orgs'] = entities_result.get('orgs', [])
                enriched['entities']['locations'] = entities_result.get('locations', [])

                # Normalize parties_present - handle both array and object formats
                parties_raw = entities_result.get('parties_present', [])
                if isinstance(parties_raw, dict):
                    # Model returned object format, extract keys where value is not null
                    parties_present = [k for k, v in parties_raw.items() if v is not None and v]
                elif isinstance(parties_raw, list):
                    # Model returned array format (expected)
                    parties_present = parties_raw
                else:
                    parties_present = []

                enriched['entities']['parties_present'] = parties_present
            else:
                warnings.append("Entity extraction failed")

        # 6c. Filter stance_by_party to only parties present
        if parties_present:
            # Keep only parties that are present, set others to 0.0
            all_parties = ['pp', 'psoe', 'vox', 'bng', 'sumar', 'podemos']
            for party in all_parties:
                if party not in parties_present:
                    enriched['classify']['stance_by_party'][party] = 0.0

        # 6d. Evaluate radar for ALL 6 topics (6 vLLM calls, skip if non-article)
        if is_non_article:
            # Neutralize signals for non-articles
            enriched['signals']['media_affinity']['affinity_contrib'] = 0.0
            enriched['audit']['axes_method'] = 'skipped'
        else:
            # Evaluate all 6 topics regardless of primary_topic
            axes_method = 'llm'
            for topic_name, topic_axes in AXES_BY_TOPIC.items():
                radar_result, tokens, latency = self.nlp_client.evaluate_topic_radar(
                    title, body, topic_name, topic_axes, parties_present
                )
                total_tokens += tokens
                total_latency += latency

                if radar_result:
                    enriched['signals']['radar'][topic_name] = radar_result
                else:
                    warnings.append(f"Radar evaluation failed for {topic_name}")
                    axes_method = 'failed'

            enriched['audit']['axes_method'] = axes_method

        # 8. Signals - Media Affinity
        enriched['signals']['media_affinity']['media_key'] = media_name
        enriched['signals']['media_affinity']['media_url'] = media_home
        if not is_non_article:
            enriched['signals']['media_affinity']['affinity_contrib'] = enriched['classify']['argument_affinity_index']
        else:
            enriched['signals']['media_affinity']['affinity_contrib'] = 0.0

        # 8b. Calculate coverage_label based on sentiment
        sentiment_score = enriched['classify']['sentiment_score']
        if sentiment_score > 0.2:
            enriched['signals']['coverage_label'] = 'positive'
        elif sentiment_score < -0.2:
            enriched['signals']['coverage_label'] = 'negative'
        else:
            enriched['signals']['coverage_label'] = 'neutral'

        # 9. Audit
        enriched['audit']['created_at'] = datetime.now(timezone.utc).isoformat()

        # Add audit tracking
        if audit_errors:
            enriched['audit']['errors'] = audit_errors
        if audit_warnings:
            enriched['audit']['warnings'] = audit_warnings
        if audit_fixed:
            enriched['audit']['fixed'] = True

        return enriched, warnings, total_tokens, total_latency

    def process_file(self, file_path: Path, root_dir: Path) -> Dict[str, Any]:
        """
        Process a single file.

        Returns:
            Dict with processing results for CSV report
        """
        self.stats['total'] += 1

        try:
            # Read input first (needed for all checks)
            data = self._read_json_file(file_path)
            if data is None:
                self.stats['errors'] += 1
                return {
                    'filepath': str(file_path),
                    'ok': False,
                    'error': 'read_failed',
                    'tokens_used': 0,
                    'latency_ms': 0
                }

            # Check if already processed (has signals.radar)
            if 'signals' in data and 'radar' in data.get('signals', {}):
                logger.info(f"Skipping {file_path} (already processed)")
                return {
                    'filepath': str(file_path),
                    'ok': True,
                    'error': 'already_processed',
                    'tokens_used': 0,
                    'latency_ms': 0
                }

            # Check date filter (only process if date <= 100 days old)
            fecha_str = data.get('fecha', '')
            if fecha_str:
                try:
                    from datetime import datetime, timezone, timedelta
                    # Parse fecha (ISO format with timezone)
                    if isinstance(fecha_str, str):
                        fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00'))
                        now = datetime.now(timezone.utc)
                        age_days = (now - fecha).days

                        if age_days > 1000:
                            logger.info(f"Skipping {file_path} (too old: {age_days} days)")
                            return {
                                'filepath': str(file_path),
                                'ok': True,
                                'error': f'too_old_{age_days}_days',
                                'tokens_used': 0,
                                'latency_ms': 0
                            }
                except Exception as e:
                    logger.debug(f"Could not parse fecha for {file_path}: {e}")

            # Check if output already exists (legacy check)
            output_path = self._get_output_path(file_path, root_dir)
            if output_path.exists() and not self.overwrite and output_path != file_path:
                logger.info(f"Skipping {file_path} (output exists)")
                return {
                    'filepath': str(file_path),
                    'ok': True,
                    'error': 'skipped',
                    'tokens_used': 0,
                    'latency_ms': 0
                }

            # Enrich
            if not self.dry_run:
                enriched, warnings, tokens, latency = self._enrich_document(data)

                # Write output unless json files must remain untouched
                if not self.json_file_constant:
                    self._write_json_file(output_path, enriched)

                    # If overwrite mode and input was .json, delete original .json file
                    if self.output_dir == root_dir and file_path.suffix == '.json' and output_path != file_path:
                        try:
                            file_path.unlink()
                            logger.debug(f"Deleted original {file_path}")
                        except Exception as e:
                            logger.warning(f"Could not delete original {file_path}: {e}")
                else:
                    logger.info(
                        "json_file_constant enabled - processed %s without writing output",
                        file_path
                    )

                if warnings:
                    self.stats['warnings'] += 1
                    logger.warning(f"{file_path}: {', '.join(warnings)}")
                else:
                    self.stats['success'] += 1

                logger.info(f"Enriched {file_path} -> {output_path}")

                return {
                    'filepath': str(file_path),
                    'ok': True,
                    'error': '' if not warnings else '; '.join(warnings),
                    'tokens_used': tokens,
                    'latency_ms': round(latency, 2)
                }
            else:
                logger.info(f"[DRY RUN] Would process {file_path}")
                return {
                    'filepath': str(file_path),
                    'ok': True,
                    'error': 'dry_run',
                    'tokens_used': 0,
                    'latency_ms': 0
                }

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error processing {file_path}: {e}")
            return {
                'filepath': str(file_path),
                'ok': False,
                'error': str(e),
                'tokens_used': 0,
                'latency_ms': 0
            }

    def _get_file_date(self, file_path: Path) -> datetime:
        """Get publication date from JSON file, fallback to modification time."""
        try:
            data = self._read_json_file(file_path)
            if data and 'fecha' in data:
                fecha_str = data['fecha']
                if isinstance(fecha_str, str):
                    # Parse ISO format date
                    fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00'))
                    # Ensure it's timezone-aware (convert to UTC if naive)
                    if fecha.tzinfo is None:
                        fecha = fecha.replace(tzinfo=timezone.utc)
                    return fecha
        except Exception:
            pass

        # Fallback to file modification time (always timezone-aware)
        return datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)

    def find_json_files(self, root_dir: Path) -> List[Path]:
        """Find all .json and .json.gz files in directory, sorted by most recent first."""
        files = []
        for pattern in ['**/*.json', '**/*.json.gz']:
            files.extend(root_dir.glob(pattern))

        logger.info(f"Sorting {len(files)} files by publication date (most recent first)...")

        # Sort by publication date from JSON (most recent first)
        # This ensures newer news articles are processed first
        return sorted(files, key=self._get_file_date, reverse=True)

    def process_directory(self, root_dir: Path, workers: int = 6):
        """Process all JSON files in directory."""
        files = self.find_json_files(root_dir)

        if not files:
            logger.warning(f"No JSON files found in {root_dir}")
            return

        logger.info(f"Found {len(files)} files to process")

        # Process files with thread pool
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self.process_file, f, root_dir): f
                for f in files
            }

            for future in as_completed(futures):
                result = future.result()
                self.report_rows.append(result)

                # Small jitter for rate limiting
                time.sleep(0.05)

    def save_report(self, report_path: Path):
        """Save CSV report."""
        with open(report_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['filepath', 'ok', 'error', 'tokens_used', 'latency_ms']
            )
            writer.writeheader()
            writer.writerows(self.report_rows)

        logger.info(f"Report saved to {report_path}")

    def print_summary(self):
        """Print processing summary."""
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Total files:      {self.stats['total']}")
        print(f"Successful:       {self.stats['success']}")
        print(f"With warnings:    {self.stats['warnings']}")
        print(f"Errors:           {self.stats['errors']}")
        print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich news JSON files with NLP analysis using vLLM"
    )

    parser.add_argument(
        '--root',
        type=str,
        default='/mnt/disco6tb/Gover.Me/rag_document_data/noticias/',
        help='Root directory containing JSON files to process (default: /mnt/disco6tb/Gover.Me/rag_document_data/noticias/)'
    )

    parser.add_argument(
        '--out',
        type=str,
        default=None,
        help='Output directory (default: None = overwrite in root with compression)'
    )

    parser.add_argument(
        '--model',
        type=str,
        default=os.getenv('VLLM_MODEL', "gemma-3-12b-it"), # Qwen3-8B-AWQ "Qwen/Qwen3-8B-AWQ" 'gemma-3-12b-it'
        help='Model name (default: VLLM_MODEL env or Qwen)'
    )

    parser.add_argument(
        '--base-url',
        type=str,
        default=os.getenv('VLLM_BASE_URL', 'http://212.69.86.224:8000/v1'), # 172.24.250.17
        help='vLLM base URL (default: VLLM_BASE_URL env or http://172.24.250.17:8000/v1)'
    )

    parser.add_argument(
        '--api-key',
        type=str,
        default=os.getenv('VLLM_API_KEY', 'sk-local-elysia-noop'),
        help='API key (default: VLLM_API_KEY env or sk-local-elysia-noop)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=3,
        help='Number of worker threads (default: 3)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode (no actual processing)'
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing output files'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--json-file-constant',
        action='store_true',
        help='Process files but keep existing JSON outputs unchanged'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate inputs
    root_dir = Path(args.root)
    if not root_dir.exists():
        logger.error(f"Root directory does not exist: {root_dir}")
        sys.exit(1)

    # If --out not provided, overwrite in root
    output_dir = Path(args.out) if args.out else root_dir

    # Initialize NLP client
    nlp_client = NLPClient(
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model
    )

    # Initialize enricher
    enricher = NewsEnricher(
        nlp_client=nlp_client,
        output_dir=output_dir,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        json_file_constant=args.json_file_constant
    )

    # Process files
    if args.out:
        logger.info(f"Starting enrichment: {root_dir} -> {output_dir}")
    else:
        logger.info(f"Starting enrichment: {root_dir} (overwrite mode with compression)")
    logger.info(f"Model: {args.model} | Workers: {args.workers}")

    start_time = time.time()
    enricher.process_directory(root_dir, workers=args.workers)
    elapsed = time.time() - start_time

    # Save report
    if not args.dry_run:
        report_path = output_dir / 'enrichment_report.csv'
        enricher.save_report(report_path)
        try:
            generate_social_network_scatter_images(
                json_path=MEDIA_AFFINITY_SOCIAL_JSON,
                output_dir=SCATTER_IMAGE_OUTPUT_DIR
            )
        except Exception as exc:  # pragma: no cover - log y continuar
            logger.warning("No se pudo generar el gráfico social_net: %s", exc)

    # Print summary
    enricher.print_summary()
    logger.info(f"Total time: {elapsed:.2f}s")


if __name__ == '__main__':
    main()
