"""
News Enricher - NLP enrichment for Spanish news articles using vLLM.

Main components:
- NewsEnricher: Main enrichment class
- NLPClient: vLLM client with caching
- Schema utilities: ID generation, normalization
- Data validators: Quality checks
"""

__version__ = "1.5.0"

from .nlp_client import NLPClient
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
from .axes_catalog import AXES_BY_TOPIC, get_axes_for_topic, has_axes
from .media_map import get_media_info, extract_domain, is_spanish_domain
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

__all__ = [
    'NLPClient',
    'create_stable_id',
    'normalize_datetime_to_utc',
    'count_words',
    'calculate_reading_time',
    'normalize_title',
    'extract_time_components',
    'get_template',
    'deep_merge',
    'AXES_BY_TOPIC',
    'get_axes_for_topic',
    'has_axes',
    'get_media_info',
    'extract_domain',
    'is_spanish_domain',
    'validate_url',
    'is_truncated_url',
    'repair_url',
    'is_non_article_content',
    'detect_paywall_overlay',
    'clean_paywall_content',
    'normalize_time_field',
    'infer_timezone_from_offset',
]
