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
            types = [str(x).lower() for x in types]
            if any(tt in ('newsarticle', 'article', 'reportagenewsarticle') for tt in types):
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

# ---- Extra: dataLayer de EL PAÍS (window.DTM.pageDataLayer) ----
def _extract_json_block(text: str, key: str) -> Optional[dict]:
    """
    Busca '"<key>": {' ... '}' y devuelve el dict JSON parseado (balanceando llaves).
    Supone que no hay llaves desbalanceadas dentro de strings.
    """
    idx = text.find(f'"{key}"')
    if idx == -1:
        return None
    # Encuentra la primera '{' luego del key:
    start = text.find('{', idx)
    if start == -1:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    block = text[start:i+1]
                    try:
                        return json.loads(block)
                    except Exception:
                        return None
    return None

def _extract_elpais_datalayer(soup: BeautifulSoup) -> dict:
    """
    Intenta extraer window.DTM.pageDataLayer como dict.
    """
    for sc in soup.find_all('script'):
        st = sc.string or ''
        if 'pageDataLayer' in st and ('window.DTM' in st or 'Object.assign(window.DTM' in st):
            obj = _extract_json_block(st, 'pageDataLayer')
            if isinstance(obj, dict):
                return obj
    return {}

# ---------- Fallback específico: EL PAÍS ----------
def aplicar_fallbacks_elpais(
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
    Extrae campos principales para EL PAÍS priorizando JSON-LD (NewsArticle),
    luego metas OG/article/author/news_keywords y, por último, dataLayer + heurístico.

    Devuelve: titulo, autor, fecha, subtitulo, contenido(list), imagen, seccion, tags(list)
    """
    contenido = contenido or []
    tags = tags or []

    dominio = (url or '').lower()
    if 'elpais.com' in dominio or ((s := soup.find('meta', attrs={'property':'og:site_name'})) and 'país' in (s.get('content','').lower())):
        print(f"📄 Fallback EL PAÍS activado para {url}.")

    # 1) JSON-LD NewsArticle
    jld = _extract_json_ld_newsarticle(soup)
    if jld:
        if not titulo:
            titulo = _clean_text(jld.get('headline') or jld.get('name'))
        if not subtitulo:
            subtitulo = _clean_text(jld.get('description'))

        if not autor:
            auth = jld.get('author')
            if isinstance(auth, list) and auth:
                a0 = auth[0]
                autor = _clean_text(a0.get('name') if isinstance(a0, dict) else str(a0))
            elif isinstance(auth, dict):
                autor = _clean_text(auth.get('name'))
            else:
                autor = _clean_text(auth if isinstance(auth, str) else None)

        if not fecha:
            fecha = _parse_iso(jld.get('datePublished')) or _parse_iso(jld.get('dateModified'))

        if not seccion:
            seccion = _clean_text(jld.get('articleSection'))

        if not imagen:
            img = jld.get('image')
            if isinstance(img, dict):
                imagen = img.get('url')
            elif isinstance(img, list) and img:
                # EL PAÍS suele dar lista de URLs
                imagen = next((x for x in img if isinstance(x, str) and x.strip()), None)
            elif isinstance(img, str):
                imagen = img

        if not contenido:
            body = jld.get('articleBody')
            if isinstance(body, str) and len(body) > 60:
                contenido = _split_article_body(body)

        if not tags:
            kw = jld.get('keywords')
            if isinstance(kw, list):
                tags = [t for t in kw if isinstance(t, str) and t.strip()]
            elif isinstance(kw, str):
                tags = [t.strip() for t in kw.split(',') if t.strip()]

    # 2) Metas OpenGraph / article:* / author / news_keywords
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
        seccion = _clean_text((soup.find('meta', attrs={'property':'article:section'}) or {}).get('content'))
    if not imagen:
        imagen = (soup.find('meta', attrs={'property':'og:image'}) or {}).get('content')

    # Tags: múltiples metas article:tag + news_keywords
    if not tags:
        metas_tag = [m.get('content','') for m in soup.find_all('meta', attrs={'property':'article:tag'})]
        news_kw_meta = (soup.find('meta', attrs={'name':'news_keywords'}) or {}).get('content','')
        raw_tags = metas_tag + news_kw_meta.split(',')
        tags = [t.strip() for t in raw_tags if t and t.strip()]

    # 3) DataLayer (pageDataLayer) como refuerzo
    if True:
        dl = _extract_elpais_datalayer(soup)
        if isinstance(dl, dict):
            if not titulo:
                titulo = _clean_text(dl.get('articleTitle'))
            if not autor:
                auths = dl.get('author') or []
                if isinstance(auths, list) and auths:
                    nm = auths[0].get('name') if isinstance(auths[0], dict) else None
                    autor = _clean_text(nm)
            if not fecha:
                fecha = _parse_iso(dl.get('publishDate') or dl.get('updateDate') or dl.get('creationDate'))
            if not seccion:
                seccion = _clean_text(dl.get('primaryCategory'))
            if not tags:
                dl_tags = dl.get('tags') or []
                tags = [t.get('name') for t in dl_tags if isinstance(t, dict) and t.get('name')]

    # 4) H1/time como redundancia y heurístico de cuerpo
    if not titulo and soup.find('h1'):
        titulo = _clean_text(soup.find('h1').get_text(strip=True))
    if not fecha and soup.find('time'):
        time_el = soup.find('time')
        fecha = _parse_iso(time_el.get('datetime')) or _clean_text(time_el.get_text(' ', strip=True))
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

def ejemplo_elpais():
    url = "https://elpais.com/espana/2025-09-14/quienes-son-los-nuevos-votantes-de-vox-datos-por-edad-sexo-y-clase-social.html"
    soup = fetch_soup(url)

    titulo = autor = fecha = subtitulo = imagen = seccion = None
    contenido, tags = [], []

    (
        titulo, autor, fecha, subtitulo,
        contenido, imagen, seccion, tags
    ) = aplicar_fallbacks_elpais(
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
    ejemplo_elpais()
