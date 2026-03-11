import re, json
from datetime import datetime
from typing import Optional, Tuple, List
import requests
from bs4 import BeautifulSoup



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
        m = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:\d{2})?)', dt)
        if m:
            try:
                return datetime.fromisoformat(m.group(1)).isoformat()
            except Exception:
                pass
    return None

def _extract_json_ld_newsarticle(soup: BeautifulSoup) -> dict:
    """
    Devuelve el primer objeto JSON-LD con @type NewsArticle/Article.
    """
    for tag in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(tag.string or '{}')
        except Exception:
            continue
        # Puede venir como objeto o lista de objetos
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            t = obj.get('@type')
            if not t:
                continue
            types = [t] if isinstance(t, str) else t
            if any(tt.lower() in ('newsarticle', 'article') for tt in types):
                return obj
    return {}

def _split_article_body(text: str) -> List[str]:
    """
    Convierte el 'articleBody' en lista de párrafos razonables.
    """
    text = _clean_text(text) or ""
    if not text:
        return []
    # Cortes por doble salto/espacio largo o punto seguido de espacios amplios
    parts = re.split(r'(?:\n{2,}|\r{2,}|(?<=\.)\s{2,})', text)
    # Filtra “párrafos” útiles
    paras = [p.strip() for p in parts if p and len(p.strip()) >= 40]
    return paras

def _extract_article_paragraphs(soup: BeautifulSoup) -> List[str]:
    """
    Heurístico genérico si no hay JSON-LD útil.
    """
    for tag in soup(['script','style','noscript','nav','footer','header','aside','form']):
        tag.decompose()

    candidates = soup.select(
        'article, main, div[itemprop="articleBody"], section[itemprop="articleBody"], '
        'div[class*="article-body"], section[class*="article-body"], '
        'div[class*="content"], section[class*="content"]'
    ) or [soup]

    best, best_score = [], 0
    for node in candidates:
        bad = any(cls in (node.get('class') or []) for cls in ['ad','ads','publicidad','banner','related','subscribe'])
        if bad:
            continue
        ps = node.find_all('p')
        long_ps, score = [], 0
        for p in ps:
            t = _clean_text(p.get_text(' ', strip=True))
            if t and len(t) >= 40 and not t.lower().startswith(('ver también','suscríbete','síguenos')):
                long_ps.append(t); score += len(t)
        if score > best_score and len(long_ps) >= 2:
            best, best_score = long_ps, score
    return best

# ---------- Fallback específico eldiario.es ----------
def aplicar_fallbacks_eldiario(
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
    Extrae campos principales para eldiario.es priorizando JSON-LD (NewsArticle).
    Devuelve: titulo, autor, fecha, subtitulo, contenido(list), imagen, seccion, tags(list)
    """
    contenido = contenido or []
    tags = tags or []

    dominio = (url or '').lower()
    if 'eldiario.es' in dominio or (soup.find('meta', attrs={'property':'og:site_name'}) and 'diario' in (soup.find('meta', attrs={'property':'og:site_name'}).get('content','').lower())):
        print(f"📄 Fallback elDiario.es activado para {url}.")

    # 1) JSON-LD NewsArticle
    jld = _extract_json_ld_newsarticle(soup)
    if jld:
        # Título
        if not titulo:
            titulo = _clean_text(jld.get('headline')) or _clean_text(jld.get('name'))
        # Subtítulo / descripción
        if not subtitulo:
            subtitulo = _clean_text(jld.get('description'))
        # Autor (puede ser lista)
        if not autor:
            auth = jld.get('author')
            if isinstance(auth, list) and auth:
                autor = _clean_text(auth[0].get('name') if isinstance(auth[0], dict) else str(auth[0]))
            elif isinstance(auth, dict):
                autor = _clean_text(auth.get('name'))
            else:
                autor = _clean_text(auth if isinstance(auth, str) else None)
        # Fecha
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
            or (soup.find('meta', attrs={'name':'cXenseParse:recs:publishtime'}) or {}).get('content')
        )
    if not seccion:
        seccion = _clean_text((soup.find('meta', attrs={'property':'article:section'}) or {}).get('content'))
    if not imagen:
        imagen = (soup.find('meta', attrs={'property':'og:image'}) or {}).get('content')

    # Tags: article:tag (varias metas) + keywords
    if not tags:
        tag_metas = [m.get('content','') for m in soup.find_all('meta', attrs={'property':'article:tag'})]
        kw_meta = (soup.find('meta', attrs={'name':'keywords'}) or {}).get('content','')
        tags = [t.strip() for t in (tag_metas + kw_meta.split(',')) if t and t.strip()]

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

def ejemplo_eldiario():
    url = "https://www.eldiario.es/andalucia/andalucia-ordena-tutorias-profesorado-familias-alumnos-sean-telematicas-caracter-general_1_12593697.html"
    url = 'https://www.eldiario.es/andalucia/montero-pone-firmes-cargos-psoe-andaluz-campana-no-crea-paso-lado_1_12580808.html'
    soup = fetch_soup(url)

    titulo = autor = fecha = subtitulo = imagen = seccion = None
    contenido, tags = [], []

    (
        titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags
    ) = aplicar_fallbacks_eldiario(
        soup=soup, url=url,
        titulo=titulo, autor=autor, fecha=fecha, subtitulo=subtitulo,
        contenido=contenido, imagen=imagen, seccion=seccion, tags=tags
    )

    print("Título:   ", titulo)
    print("Autor:    ", autor)
    print("Fecha:    ", fecha)        # ISO-8601 si venía en JSON-LD/metas
    print("Subtítulo:", subtitulo)
    print("Sección:  ", seccion)
    print("Imagen:   ", imagen)
    print("Tags:     ", tags[:10])

    print("\nContenido (primeros 3 párrafos):")
    for p in contenido[:3]:
        print("-", p)
    print(f"\nTotal de párrafos extraídos: {len(contenido)}")

if __name__ == "__main__":
    ejemplo_eldiario()
