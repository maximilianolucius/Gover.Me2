import re, json
from datetime import datetime
from typing import Optional, Tuple, List
from bs4 import BeautifulSoup
import requests

# ---------- Utilidades ----------
def _clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None

def _parse_iso(dt: Optional[str]) -> Optional[str]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace('Z', '+00:00')).isoformat()
    except Exception:
        m = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:\d{2})?)', dt or '')
        if m:
            try:
                return datetime.fromisoformat(m.group(1)).isoformat()
            except Exception:
                return None
    return None

def _extract_json_ld_newsarticle(soup: BeautifulSoup) -> dict:
    """Devuelve el primer objeto JSON-LD con @type NewsArticle/Article."""
    for tag in soup.find_all('script', type='application/ld+json'):
        raw = tag.string or ''
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for obj in items:
            t = obj.get('@type')
            types = [t] if isinstance(t, str) else (t or [])
            if any(tt.lower() in ('newsarticle', 'article') for tt in types):
                return obj
    return {}

def _split_article_body(text: str) -> List[str]:
    """Convierte 'articleBody' en lista de párrafos razonables."""
    txt = _clean_text(text) or ""
    if not txt:
        return []
    parts = re.split(r'(?:\n{2,}|\r{2,}|(?<=\.)\s{2,})', txt)
    return [p.strip() for p in parts if p and len(p.strip()) >= 40]

def _extract_article_paragraphs(soup: BeautifulSoup) -> List[str]:
    """Heurístico si no hay JSON-LD útil."""
    for tag in soup(['script','style','noscript','nav','footer','header','aside','form']):
        tag.decompose()

    candidates = soup.select(
        'article, main, div[itemprop="articleBody"], section[itemprop="articleBody"], '
        'div[class*="article-body"], section[class*="article-body"], '
        'div[class*="content"], section[class*="content"]'
    ) or [soup]

    best, best_score = [], 0
    for node in candidates:
        ps = node.find_all('p')
        long_ps, score = [], 0
        for p in ps:
            t = _clean_text(p.get_text(' ', strip=True))
            if t and len(t) >= 40 and not t.lower().startswith(('ver también','suscríbete','síguenos')):
                long_ps.append(t); score += len(t)
        if score > best_score and len(long_ps) >= 2:
            best, best_score = long_ps, score
    return best

# ---------- Fallback específico Ideal.es ----------
def aplicar_fallbacks_ideal(
    soup: BeautifulSoup,
    url: str,
    titulo: Optional[str] = None,
    autor: Optional[str] = None,
    fecha: Optional[str] = None,
    subtitulo: Optional[str] = None,
    contenido: Optional[List[str]] = None,
    imagen: Optional[str] = None,
    seccion: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], List[str], Optional[str], Optional[str], List[str]]:
    """
    Extrae campos principales para Ideal.es priorizando JSON-LD (NewsArticle),
    luego metas OG/article y, por último, un heurístico de párrafos.

    Devuelve: titulo, autor, fecha, subtitulo, contenido(list), imagen, seccion, tags(list)
    """
    contenido = contenido or []
    tags = tags or []

    dominio = (url or '').lower()
    if 'ideal.es' in dominio or ((s := soup.find('meta', attrs={'property':'og:site_name'})) and 'ideal' in (s.get('content','').lower())):
        print(f"📄 Fallback Ideal.es activado para {url}.")

    # 1) JSON-LD NewsArticle
    jld = _extract_json_ld_newsarticle(soup)
    if jld:
        # Título / descripción
        if not titulo:
            titulo = _clean_text(jld.get('headline') or jld.get('name'))
        if not subtitulo:
            subtitulo = _clean_text(jld.get('description'))

        # Autor (array, dict o string)
        if not autor:
            auth = jld.get('author')
            if isinstance(auth, list) and auth:
                a0 = auth[0]
                autor = _clean_text(a0.get('name') if isinstance(a0, dict) else str(a0))
            elif isinstance(auth, dict):
                autor = _clean_text(auth.get('name'))
            else:
                autor = _clean_text(auth if isinstance(auth, str) else None)

        # Fechas
        if not fecha:
            fecha = _parse_iso(jld.get('datePublished')) or _parse_iso(jld.get('dateModified'))

        # Sección
        if not seccion:
            seccion = _clean_text(jld.get('articleSection'))

        # Imagen
        if not imagen:
            img = jld.get('image')
            if isinstance(img, dict):
                imagen = img.get('url')
            elif isinstance(img, list) and img:
                imagen = img[0] if isinstance(img[0], str) else img[0].get('url')
            elif isinstance(img, str):
                imagen = img

        # Contenido
        if not contenido:
            body = jld.get('articleBody')
            if isinstance(body, str) and len(body) > 60:
                contenido = _split_article_body(body)

        # Tags (keywords suele ser lista)
        if not tags:
            kw = jld.get('keywords')
            if isinstance(kw, list):
                tags = [t for t in (kw or []) if isinstance(t, str) and t.strip()]
            elif isinstance(kw, str):
                tags = [t.strip() for t in kw.split(',') if t.strip()]

    # 2) Metas OpenGraph / article:* / author / keywords
    if not titulo:
        titulo = _clean_text(
            (soup.find('meta', attrs={'property':'og:title'}) or {}).get('content')
            or (soup.find('meta', attrs={'name':'title'}) or {}).get('content')
        )
    if not subtitulo:
        subtitulo = _clean_text(
            (soup.find('meta', attrs={'property':'og:description'}) or {}).get('content')
            or (soup.find('meta', attrs={'name':'description'}) or {}).get('content')
        )
    if not autor:
        autor = _clean_text((soup.find('meta', attrs={'name':'author'}) or {}).get('content'))
    if not fecha:
        fecha = _parse_iso(
            (soup.find('meta', attrs={'property':'article:published_time'}) or {}).get('content')
            or (soup.find('meta', attrs={'name':'DC.date.issued'}) or {}).get('content')
            or (soup.find('meta', attrs={'name':'date'}) or {}).get('content')
        )
    if not seccion:
        seccion = _clean_text(
            (soup.find('meta', attrs={'property':'article:section'}) or {}).get('content')
            or (soup.find('meta', attrs={'property':'mrf:sections'}) or {}).get('content')
        )
    if not imagen:
        imagen = (soup.find('meta', attrs={'property':'og:image'}) or {}).get('content')

    # Tags: article:tag (pueden repetirse) + keywords/news_keywords + mrf:tags
    if not tags:
        metas_tag = [m.get('content','') for m in soup.find_all('meta', attrs={'property':'article:tag'})]
        kw_meta = (soup.find('meta', attrs={'name':'keywords'}) or {}).get('content','')
        news_kw_meta = (soup.find('meta', attrs={'name':'news_keywords'}) or {}).get('content','')
        mrf_tags = (soup.find('meta', attrs={'property':'mrf:tags'}) or {}).get('content','')
        raw_tags = metas_tag + kw_meta.split(',') + news_kw_meta.split(',') + re.split(r'[,\|]', mrf_tags or '')
        tags = [t.strip() for t in raw_tags if t and t.strip()]

    # 3) H1 / time como redundancia
    if not titulo and soup.find('h1'):
        titulo = _clean_text(soup.find('h1').get_text(strip=True))
    if not fecha and soup.find('time'):
        time_el = soup.find('time')
        fecha = _parse_iso(time_el.get('datetime')) or _clean_text(time_el.get_text(' ', strip=True))

    # 4) Cuerpo si aún falta (heurístico)
    if not contenido:
        contenido = _extract_article_paragraphs(soup)

    return titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags

def fetch_soup(url: str) -> BeautifulSoup:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-ES,es;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def ejemplo_ideal():
    url = "https://www.ideal.es/granada/provincia-granada/pelea-deja-herido-grave-fiestas-ogijares-20250913125634-nt.html"
    url = "https://www.ideal.es/granada/nueva-jornada-retrasos-dos-horas-trenes-madrid-20250913190216-nt.html"
    soup = fetch_soup(url)

    titulo = autor = fecha = subtitulo = imagen = seccion = None
    contenido, tags = [], []

    (
        titulo, autor, fecha, subtitulo,
        contenido, imagen, seccion, tags
    ) = aplicar_fallbacks_ideal(
        soup=soup, url=url,
        titulo=titulo, autor=autor, fecha=fecha, subtitulo=subtitulo,
        contenido=contenido, imagen=imagen, seccion=seccion, tags=tags
    )

    print("Título:   ", titulo)
    print("Autor:    ", autor)
    print("Fecha:    ", fecha)
    print("Subtítulo:", subtitulo)
    print("Sección:  ", seccion)
    print("Imagen:   ", imagen)
    print("Tags:     ", tags[:10])

    print("\nContenido (primeros 3 párrafos):")
    for p in contenido[:3]:
        print("-", p)
    print(f"\nTotal de párrafos extraídos: {len(contenido)}")

if __name__ == "__main__":
    ejemplo_ideal()
