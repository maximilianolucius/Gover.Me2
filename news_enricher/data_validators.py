"""
Data validation and cleaning utilities.
"""
import re
from typing import Tuple, Optional, List
from urllib.parse import urlparse
import unicodedata


def validate_url(url: str) -> Tuple[bool, Optional[str]]:
    """
    Validate URL format.

    Returns:
        (is_valid, reason_if_invalid)
    """
    if not url:
        return False, "empty_url"

    # Regex: ^https?://[domain]/path$
    pattern = r'^https?://[A-Za-z0-9\.\-]+(?:\:[0-9]+)?/.*$'
    if not re.match(pattern, url):
        return False, "invalid_format"

    # Check for truncation: ends with /YYYY or /YYYY/ without slug
    # Heuristic: URL ends with year but no article identifier
    truncation_pattern = r'/\d{4}/?$'
    if re.search(truncation_pattern, url):
        return False, "truncated_year"

    # Check if URL ends without proper article identifier
    # Good: .html, .php, slug-text, article ID
    # Bad: just /category/ or /year/month/
    if url.endswith('/') and not any(x in url for x in ['.html', '.php', '-']):
        # Might be category/index page
        parts = url.rstrip('/').split('/')
        if len(parts) <= 4:  # http://domain/category/ is suspicious
            return False, "possible_index"

    return True, None


def is_truncated_url(url: str) -> bool:
    """Check if URL appears truncated."""
    if not url:
        return True

    # Missing protocol
    if not url.startswith(('http://', 'https://')):
        return True

    # Ends with year only
    if re.search(r'/\d{4}/?$', url):
        return True

    # Too short (less than typical article URL)
    if len(url) < 30:
        return True

    return False


def repair_url(url: str, domain: str, title: str) -> Optional[str]:
    """
    Attempt conservative URL reconstruction.

    Args:
        url: Original (possibly truncated) URL
        domain: Source domain
        title: Article title

    Returns:
        Repaired URL or None if can't repair safely
    """
    if not domain or not title:
        return None

    # Only attempt repair if we have a base
    if url and url.startswith(('http://', 'https://')):
        # URL has protocol but might be incomplete
        # Don't attempt repair - too risky
        return None

    # Create slug from title
    slug = slugify(title)
    if not slug:
        return None

    # Conservative reconstruction
    repaired = f"https://{domain}/{slug}.html"

    return repaired


def slugify(text: str, max_length: int = 100) -> str:
    """
    Create URL-safe slug from text.

    Args:
        text: Text to slugify
        max_length: Maximum slug length

    Returns:
        URL-safe slug
    """
    if not text:
        return ""

    # Lowercase
    slug = text.lower()

    # Remove accents
    slug = unicodedata.normalize('NFD', slug)
    slug = ''.join(c for c in slug if unicodedata.category(c) != 'Mn')

    # Keep only alphanumeric and spaces
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)

    # Replace spaces with hyphens
    slug = re.sub(r'\s+', '-', slug)

    # Remove multiple hyphens
    slug = re.sub(r'-+', '-', slug)

    # Trim hyphens from ends
    slug = slug.strip('-')

    # Limit length
    if len(slug) > max_length:
        slug = slug[:max_length].rsplit('-', 1)[0]

    return slug


def is_non_article_content(body: str, url: str, word_count: int) -> Tuple[bool, Optional[str]]:
    """
    Detect if content is not an actual article (index, navigation, etc.).

    Args:
        body: Article body text
        url: Article URL
        word_count: Word count

    Returns:
        (is_non_article, reason)
    """
    # Rule 1: Too short
    if word_count < 120:
        return True, "too_short"

    # Rule 2: URL patterns for index pages
    index_patterns = [
        r'/municipio/',
        r'/elecciones/resultados/',
        r'/resultados/',
        r'/tema/',
        r'/tag/',
        r'/autor/',
        r'/categoria/',
        r'/seccion/$',
        r'/archivo/',
        r'/indice/',
    ]

    for pattern in index_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True, f"index_url_pattern:{pattern}"

    # Rule 3: Content structure suggests index
    # Many short lines (bullets/links)
    lines = [l.strip() for l in body.split('\n') if l.strip()]
    if len(lines) > 20:
        short_lines = [l for l in lines if len(l) < 50]
        if len(short_lines) / len(lines) > 0.7:
            return True, "index_structure_bullets"

    # Rule 4: Repetitive patterns (common in lists)
    # Check for repeated short phrases
    if len(lines) > 10:
        unique_starts = set([l[:20] for l in lines if len(l) > 20])
        if len(unique_starts) < len(lines) * 0.3:
            return True, "repetitive_content"

    return False, None


def detect_paywall_overlay(body: str) -> Tuple[bool, List[str]]:
    """
    Detect and identify paywall/overlay content blocks.

    Args:
        body: Article body text

    Returns:
        (has_paywall, list_of_paywall_paragraphs)
    """
    paywall_blocks = []

    # Common Spanish paywall patterns
    paywall_patterns = [
        r'¿Quieres añadir otro usuario a tu suscripción\?',
        r'Si continúas leyendo en este dispositivo',
        r'cambia tu suscripción a la modalidad Premium',
        r'¿Tienes una suscripción de empresa\?',
        r'te recomendamos cambiar tu contraseña',
        r'términos y condiciones de la suscripción',
        r'Accede aquí para contratar más cuentas',
        r'este mensaje se mostrará en tu dispositivo',
        r'Suscríbete para seguir leyendo',
        r'Hazte suscriptor',
        r'Regístrate gratis',
        r'contenido exclusivo para suscriptores',
    ]

    paragraphs = body.split('\n\n')
    detected_blocks = []

    for para in paragraphs:
        para_clean = para.strip()
        if not para_clean:
            continue

        # Check if paragraph matches paywall pattern
        for pattern in paywall_patterns:
            if re.search(pattern, para_clean, re.IGNORECASE):
                detected_blocks.append(para_clean)
                break

    has_paywall = len(detected_blocks) > 0

    return has_paywall, detected_blocks


def clean_paywall_content(body: str) -> str:
    """
    Remove paywall/overlay blocks from body text.

    Args:
        body: Original body text

    Returns:
        Cleaned body text
    """
    has_paywall, paywall_blocks = detect_paywall_overlay(body)

    if not has_paywall:
        return body

    cleaned = body
    for block in paywall_blocks:
        cleaned = cleaned.replace(block, '')

    # Clean up multiple newlines
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

    return cleaned.strip()


def normalize_time_field(value: any) -> Optional[str]:
    """
    Normalize time field to None if empty/zero.

    Args:
        value: Time field value

    Returns:
        None if empty/zero, otherwise ISO string
    """
    if value is None:
        return None

    if isinstance(value, str):
        if value == "" or value == "0":
            return None
        return value

    if isinstance(value, int):
        if value == 0:
            return None
        return None  # Unexpected int

    return None


def infer_timezone_from_offset(offset_str: str) -> Optional[str]:
    """
    Infer timezone name from offset.

    Args:
        offset_str: Offset like "+01:00", "+02:00"

    Returns:
        Timezone name or None
    """
    if not offset_str:
        return None

    # Spanish time: CET/CEST is +01:00 (winter) or +02:00 (summer)
    if offset_str in ['+01:00', '+0100', '+01']:
        return 'Europe/Madrid'
    if offset_str in ['+02:00', '+0200', '+02']:
        return 'Europe/Madrid'

    return None
