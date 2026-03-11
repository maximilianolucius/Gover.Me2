import requests
import re, json
from datetime import datetime
from bs4 import BeautifulSoup

# Utilidad: limpieza simple
def _clean_text(s):
    if not s: return None
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None

# Utilidad: intenta parsear ISO 8601 con varias variantes
def _parse_iso(dt):
    if not dt: return None
    try:
        return datetime.fromisoformat(dt.replace('Z', '+00:00')).isoformat()
    except Exception:
        # Otras variantes comunes
        m = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:\d{2})?)', dt)
        if m:
            try:
                return datetime.fromisoformat(m.group(1)).isoformat()
            except Exception:
                pass
    return None

# Utilidad: extraer primer JSON del dataLayer (window.dataLayer.push({...}))
def _extract_datalayer(soup):
    dl = {}
    script = soup.find('script', attrs={'data-hid':'dataLayer'})
    if not script or not script.string:
        return dl
    # Captura el objeto pasado a push(...)
    m = re.search(r'push\(\s*(\{.*?\})\s*\)\s*;', script.string, re.S)
    if not m:
        return dl
    js = m.group(1)
    # Reemplazos mínimos para que sea JSON válido
    js = re.sub(r'\bundefined\b', 'null', js)
    try:
        dl = json.loads(js)
    except Exception:
        # Fallback: intenta sanear comillas sueltas (no debería hacer falta)
        js2 = js.replace("'", '"')
        try:
            dl = json.loads(js2)
        except Exception:
            return {}
    return dl or {}

# Heurístico: buscar el contenedor con más texto de <p> "largos"
def _extract_article_paragraphs(soup):
    # Evitar zonas de ruido
    for tag in soup(['script','style','noscript','nav','footer','header','aside','form']):
        tag.decompose()

    candidates = soup.select(
        'article, main, div[itemprop="articleBody"], section[itemprop="articleBody"], '
        'div[class*="article-body"], section[class*="article-body"], '
        'div[class*="contenido"], section[class*="contenido"], '
        'div[class*="content"], section[class*="content"]'
    )
    # Si no se detectan candidatos, usar todo el documento como último recurso
    if not candidates:
        candidates = [soup]

    best = None
    best_score = 0
    for node in candidates:
        # Evita contenedores de publicidad y módulos irrelevantes
        bad = any(cls in (node.get('class') or []) for cls in ['ad','ads','publicidad','banner','related','subscribe'])
        if bad:
            continue
        # Cuenta párrafos "largos"
        ps = node.find_all('p')
        long_ps = []
        score = 0
        for p in ps:
            t = _clean_text(p.get_text(' ', strip=True))
            if not t:
                continue
            # filtra líneas muy cortas (menús, breadcrumbs, etc.)
            if len(t) >= 40 and not t.lower().startswith(('ver también', 'suscríbete', 'síguenos')):
                long_ps.append(t)
                score += len(t)
        if score > best_score and len(long_ps) >= 2:
            best = long_ps
            best_score = score

    return best or []

def aplicar_fallbacks_abc_diariosur_elcorreo(soup, url, titulo=None, autor=None, fecha=None, subtitulo=None, contenido=None, imagen=None, seccion=None, tags=None):
    """
    Amplía tu fallback ABC/Diario Sur y añade soporte para El Correo de Andalucía (Prensa Ibérica).
    Devuelve los mismos nombres de variables que usas en tu pipeline.
    """
    contenido = contenido or []
    tags = tags or []

    dominio = (url or '').lower()

    # ---------- 1) Fallback ABC / Diario Sur (Vocento) ----------
    if ('diariosur.es' in dominio) or ('abc.es' in dominio) or soup.find('h1', class_='v-a-t'):
        print(f"📄 Fallback ABC/Diario Sur activado para {url}.")

        # Título
        if not titulo:
            h1 = soup.find('h1', class_='v-a-t') or soup.find('h1')
            if h1: titulo = _clean_text(h1.get_text(strip=True))

        # Autor
        if not autor:
            autor_elem = soup.find('p', class_='v-mdl-ath__p v-mdl-ath__p--2')
            if autor_elem:
                a = autor_elem.find('a')
                autor = _clean_text((a or autor_elem).get_text(strip=True))
        if not autor:
            meta_autor = soup.find('meta', attrs={'name': 'author'})
            if meta_autor:
                autor = _clean_text(meta_autor.get('content'))

        # Fecha
        if not fecha:
            time_elem = soup.find('time', class_='v-mdl-ath__tm') or soup.find('time')
            if time_elem:
                fecha = _parse_iso(time_elem.get('datetime')) or _clean_text(time_elem.get_text(strip=True))
        if not fecha:
            meta_fecha = soup.find('meta', attrs={'property': 'article:published_time'})
            if meta_fecha:
                fecha = _parse_iso(meta_fecha.get('content'))

        # Subtítulo
        if not subtitulo:
            h2 = soup.find('h2', class_='v-a-sub-t') or soup.find('h2')
            if h2: subtitulo = _clean_text(h2.get_text(strip=True))
        if not subtitulo:
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                subtitulo = _clean_text(meta_desc.get('content'))

        # Contenido
        if not contenido:
            for p in soup.find_all('p', class_='v-p'):
                t = _clean_text(p.get_text(strip=True))
                if t and len(t) > 20: contenido.append(t)
        if not contenido:
            paywall_area = soup.find('div', class_='paywall')
            if paywall_area:
                for p in paywall_area.find_all('p'):
                    t = _clean_text(p.get_text(strip=True))
                    if t and len(t) > 20: contenido.append(t)

        # Imagen
        if not imagen:
            og_img = soup.find('meta', attrs={'property':'og:image'})
            if og_img: imagen = og_img.get('content')

        return titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags

    # ---------- 2) Fallback El Correo de Andalucía / Prensa Ibérica ----------
    if ('elcorreoweb.es' in dominio) or ('prensaiberica' in (soup.get_text()[:5000].lower())):
        print(f"📄 Fallback Prensa Ibérica (El Correo) activado para {url}.")

        # Metas y og:*
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
            autor = _clean_text(
                (soup.find('meta', attrs={'name':'author'}) or {}).get('content')
                or (soup.find('meta', attrs={'property':'mrf:authors'}) or {}).get('content')
            )
        if not fecha:
            fecha = _parse_iso(
                (soup.find('meta', attrs={'property':'article:published_time'}) or {}).get('content')
                or (soup.find('meta', attrs={'property':'article:modified_time'}) or {}).get('content')
            )
        if not seccion:
            seccion = _clean_text((soup.find('meta', attrs={'property':'article:section'}) or {}).get('content'))
        if not tags:
            # article:tag (coma), keywords (coma) y dataLayer (pipe)
            tag_meta = (soup.find('meta', attrs={'property':'article:tag'}) or {}).get('content') or ''
            kw_meta = (soup.find('meta', attrs={'name':'keywords'}) or {}).get('content') or ''
            tags = [t.strip() for t in (tag_meta.split(',') + kw_meta.split(',')) if t.strip()]

        if not imagen:
            imagen = ((soup.find('meta', attrs={'property':'og:image'}) or {}).get('content')) or None

        # dataLayer (suele traer h1, autor, fechas, sección y tags)
        dl = _extract_datalayer(soup)
        if isinstance(dl, dict):
            content = (dl.get('content') or {})
            person = (content.get('person') or dl.get('person') or {})
            # Título del h1 (en minúsculas en algunos sitios)
            if not titulo:
                t = content.get('h1') or (dl.get('page') or {}).get('h1')
                titulo = _clean_text(t)
            # Autor
            if not autor:
                autor = _clean_text(person.get('author'))
            # Fechas
            if not fecha:
                date_pub = (content.get('date') or {}).get('publication') or (dl.get('date') or {}).get('publication')
                fecha = _parse_iso(date_pub) or _clean_text(date_pub)
            # Sección
            if not seccion:
                sec = (content.get('section') or {}).get('level_1') or content.get('category')
                seccion = _clean_text(sec)
            # Tags
            if not tags:
                dl_tags = (content.get('tag') or '')
                # Suele venir separado por pipes "algeciras|droga|carcel"
                if isinstance(dl_tags, str) and dl_tags:
                    tags = [t.strip() for t in re.split(r'[|,]', dl_tags) if t.strip()]

        # Título directo en <h1> como último recurso
        if not titulo:
            h1 = soup.find('h1')
            if h1: titulo = _clean_text(h1.get_text(strip=True))

        # Fecha en <time>
        if not fecha:
            time_el = soup.find('time')
            if time_el:
                fecha = _parse_iso(time_el.get('datetime')) or _clean_text(time_el.get_text(' ', strip=True))

        # Cuerpo del artículo (heurístico robusto)
        if not contenido:
            contenido = _extract_article_paragraphs(soup)

        # Paywall / contenedor cerrado como extra
        if not contenido:
            paywall = soup.find(id='paywall') or soup.find('div', id='closeContentContainer') or soup.find('div', {'data-id':'closeContentContainer'})
            if paywall:
                for p in paywall.find_all('p'):
                    t = _clean_text(p.get_text(' ', strip=True))
                    if t and len(t) > 40:
                        contenido.append(t)

        return titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags

    # ---------- 3) Fallback genérico ----------
    print(f"📄 Fallback genérico activado para {url}.")
    if not titulo:
        titulo = _clean_text(
            (soup.find('meta', attrs={'property':'og:title'}) or {}).get('content')
            or (soup.find('meta', attrs={'name':'title'}) or {}).get('content')
        ) or (soup.find('h1').get_text(strip=True) if soup.find('h1') else None)

    if not autor:
        autor = _clean_text(
            (soup.find('meta', attrs={'name':'author'}) or {}).get('content')
        ) or _clean_text((soup.find(attrs={'itemprop':'author'}) or {}).get_text(strip=True) if soup.find(attrs={'itemprop':'author'}) else None)

    if not fecha:
        fecha = _parse_iso(
            (soup.find('meta', attrs={'property':'article:published_time'}) or {}).get('content')
        )
        if not fecha:
            time_el = soup.find('time')
            if time_el:
                fecha = _parse_iso(time_el.get('datetime')) or _clean_text(time_el.get_text(' ', strip=True))

    if not subtitulo:
        subtitulo = _clean_text(
            (soup.find('meta', attrs={'property':'og:description'}) or {}).get('content')
            or (soup.find('meta', attrs={'name':'description'}) or {}).get('content')
        )

    if not contenido:
        contenido = _extract_article_paragraphs(soup)

    if not imagen:
        og_img = soup.find('meta', attrs={'property':'og:image'})
        if og_img: imagen = og_img.get('content')

    return titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags

def extraer_articulo(url: str) -> dict:
    soup = fetch_soup(url)

    titulo = autor = fecha = subtitulo = imagen = seccion = None
    contenido, tags = [], []

    (titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags) = \
        aplicar_fallbacks_abc_diariosur_elcorreo(
            soup, url, titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags
        )
    return titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags

def fetch_soup(url: str) -> BeautifulSoup:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-ES,es;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

if __name__ == '__main__':
    titulo, autor, fecha, subtitulo, contenido, imagen, seccion, tags = extraer_articulo("https://www.elcorreoweb.es/andalucia/2025/09/11/tres-reclusos-mueren-sobredosis-carcel-121481841.html")
    print("Título:   ", titulo)
    print("Autor:    ", autor)
    print("Fecha:    ", fecha)  # ISO-8601 si venía en meta/time
    print("Subtítulo:", subtitulo)
    print("Sección:  ", seccion)
    print("Imagen:   ", imagen)
    print("Tags:     ", tags)

    print("\nContenido (primeros 3 párrafos):")
    for p in contenido[:3]:
        print("-", p)
    print(f"\nTotal de párrafos extraídos: {len(contenido)}")