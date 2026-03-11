"""
Media domain mapping and utilities.
"""
from typing import Tuple, Optional
from urllib.parse import urlparse

# Domain overrides: domain -> (name, home_url)
NAME_OVERRIDES = {
    "elpais.com": ("El País", "https://elpais.com/?ed=es"),
    "diariosur.es": ("Diario SUR", "https://www.diariosur.es/"),
}

# Special case: ABC with region detection
ABC_REGIONS = {
    "/sevilla/": ("ABC Sevilla", "https://www.abc.es/sevilla/"),
}
ABC_DEFAULT = ("ABC", "https://www.abc.es/")


def get_media_info(domain: str, url_original: str = "") -> Tuple[str, str]:
    """
    Return (media_name, media_home_url) for a given domain.

    Args:
        domain: Domain name (e.g., "elpais.com")
        url_original: Full URL for special cases (e.g., ABC regions)

    Returns:
        Tuple of (name, home_url)
    """
    # Check direct overrides
    if domain in NAME_OVERRIDES:
        return NAME_OVERRIDES[domain]

    # Special case: ABC with region detection
    if domain == "abc.es":
        url_lower = url_original.lower()
        for region_path, (name, home) in ABC_REGIONS.items():
            if region_path in url_lower:
                return (name, home)
        return ABC_DEFAULT

    # Default: capitalize domain and use https
    name = domain.replace(".es", "").replace(".com", "").capitalize()
    home_url = f"https://{domain}/"

    return (name, home_url)


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]
        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def is_spanish_domain(domain: str) -> bool:
    """
    Check if a domain is Spanish (for timezone/geo detection).

    Returns True if:
    - Domain ends with .es
    - Domain is a known Spanish media outlet
    """
    if domain.endswith('.es'):
        return True

    # Known Spanish domains (even if .com)
    spanish_domains = {
        'elpais.com',
        'elmundo.es',
        'abc.es',
        'lavanguardia.com',
        '20minutos.es',
        'elconfidencial.com',
        'elespanol.com',
        'publico.es',
        'eldiario.es',
        'okdiario.com',
        'diariosur.es',
        'elperiodico.com',
        'europapress.es',
        'rtve.es',
    }

    return domain in spanish_domains
