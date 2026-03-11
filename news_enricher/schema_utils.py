"""
Schema merging and validation utilities.
"""
import hashlib
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict
import re


def create_stable_id(url: str, title: str, date: str) -> str:
    """
    Create stable ID from URL, title and date.
    Format: sha1:<hash>
    """
    content = f"{url}|{title}|{date}"
    hash_hex = hashlib.sha1(content.encode('utf-8')).hexdigest()
    return f"sha1:{hash_hex}"


def normalize_datetime_to_utc(date_str: str, tz_hint: str = None) -> str:
    """
    Normalize date string to ISO8601 UTC format.
    Returns empty string if parsing fails.
    """
    if not date_str:
        return ""

    try:
        # Try parsing ISO format first
        if 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            # Try common formats
            for fmt in [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
            else:
                return ""

        # Convert to UTC if not aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat()
    except Exception:
        return ""


def count_words(text: str) -> int:
    """Count words in text."""
    if not text:
        return 0
    return len(re.findall(r'\w+', text))


def calculate_reading_time(word_count: int) -> int:
    """Calculate reading time in minutes (200 words/min)."""
    if word_count == 0:
        return 0
    return max(1, (word_count + 199) // 200)  # Round up


def normalize_title(title: str) -> str:
    """
    Normalize title: lowercase, remove accents, keep alphanumeric and spaces.

    Examples:
        "El Título Está Aquí" -> "el titulo esta aqui"
        "¿Pregunta?" -> "pregunta"
    """
    if not title:
        return ""

    # Lowercase
    normalized = title.lower()

    # Remove accents/diacritics
    normalized = unicodedata.normalize('NFD', normalized)
    normalized = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

    # Keep only alphanumeric and spaces
    normalized = re.sub(r'[^a-z0-9\s]', ' ', normalized)

    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized


def extract_time_components(dt_str: str) -> Dict[str, int]:
    """Extract year, month, day, iso_week from ISO datetime."""
    if not dt_str:
        return {"year": 0, "month": 0, "day": 0, "iso_week": 0}

    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "iso_week": dt.isocalendar()[1]
        }
    except Exception:
        return {"year": 0, "month": 0, "day": 0, "iso_week": 0}


def get_template() -> Dict[str, Any]:
    """Return the base template structure."""
    return {
        "schema_version": "news_v2",
        "id": "",
        "source": {
            "domain": "",
            "section": None,
            "paywall": None,
            "region_hint": None,
            "media": {"name": "", "home_url": ""}
        },
        "time": {
            "published_utc": "",
            "published_tz": None,
            "updated_utc": None,
            "extracted_utc": "",
            "year": 0,
            "month": 0,
            "day": 0,
            "iso_week": 0
        },
        "text": {
            "lang": "es",
            "word_count": 0,
            "reading_time_min": 0,
            "title_norm": "",
            "body_plain": "",
            "quotes": []
        },
        "entities": {
            "persons": [],
            "orgs": [],
            "locations": [],
            "parties_present": []
        },
        "classify": {
            "primary_topic": "",
            "subtopics": [],
            "sentiment_label": "neutral",
            "sentiment_score": 0.0,
            "stance_by_party": {
                "pp": 0.0,
                "psoe": 0.0,
                "vox": 0.0,
                "bng": 0.0,
                "sumar": 0.0,
                "podemos": 0.0
            },
            "argument_affinity_index": 0.0
        },
        "signals": {
            "radar": {
                "vivienda": {
                    "pp": [None, None, None, None, None],
                    "vox": [None, None, None, None, None],
                    "psoe": [None, None, None, None, None],
                    "programa": [None, None, None, None, None]
                },
                "economia": {
                    "pp": [None, None, None, None, None],
                    "vox": [None, None, None, None, None],
                    "psoe": [None, None, None, None, None],
                    "programa": [None, None, None, None, None]
                },
                "sanidad": {
                    "pp": [None, None, None, None, None],
                    "vox": [None, None, None, None, None],
                    "psoe": [None, None, None, None, None],
                    "programa": [None, None, None, None, None]
                },
                "seguridad": {
                    "pp": [None, None, None, None, None],
                    "vox": [None, None, None, None, None],
                    "psoe": [None, None, None, None, None],
                    "programa": [None, None, None, None, None]
                },
                "educacion": {
                    "pp": [None, None, None, None, None],
                    "vox": [None, None, None, None, None],
                    "psoe": [None, None, None, None, None],
                    "programa": [None, None, None, None, None]
                },
                "transporte": {
                    "pp": [None, None, None, None, None],
                    "vox": [None, None, None, None, None],
                    "psoe": [None, None, None, None, None],
                    "programa": [None, None, None, None, None]
                }
            },
            "media_affinity": {
                "media_key": "",
                "media_url": "",
                "affinity_contrib": 0.0,
                "reach_hint": None,
                "weight": 1.0
            },
            "coverage_label": "neutral"
        },
        "links": {
            "outbound": [],
            "media": [],
            "related_ids": []
        },
        "geo": {
            "country": None,
            "adm1": None,
            "city": None
        },
        "nlp": {
            "model": "",
            "summary_abstractive": "",
            "bullets_extractive": [],
            "keywords": []
        },
        "audit": {
            "pipeline_version": "etl-news-1.0.0",
            "created_at": ""
        }
    }


def deep_merge(base: Dict, overlay: Dict) -> Dict:
    """
    Merge overlay into base, preserving existing values in base.
    Only adds missing keys from overlay.
    """
    result = base.copy()

    for key, value in overlay.items():
        if key not in result:
            result[key] = value
        elif isinstance(value, dict) and isinstance(result[key], dict):
            result[key] = deep_merge(result[key], value)

    return result
