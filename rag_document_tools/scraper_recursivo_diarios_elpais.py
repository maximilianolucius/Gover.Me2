import os
import json
import gzip
import time
import random
import hashlib
import argparse
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import socket

"""
Scraper recursivo para El País (sección España) con login por Selenium y
rastreo mediante requests + cookies de sesión.

- Inicia sesión con Selenium para evitar paywall
- Exporta cookies de Selenium a requests.Session
- Recorre enlaces que contengan "elpais.com/espana/"
- Extrae y guarda cada noticia como JSON comprimido (.json.gz)
- Estructura robusta y tolerante a cambios menores en el HTML

Ejemplos:

  python scraper_elpais_recursivo.py \
    --email "usuario@dominio" --password "********" \
    --url "https://elpais.com/espana/" \
    --directorio "./rag_document_data/noticias/" \
    --enlaces 200 --depth 5

Variables de entorno opcionales:
  ELPAIS_EMAIL, ELPAIS_PASSWORD

Notas:
- Requiere ChromeDriver en PATH (o webdriver-manager si lo prefieres).
- Si ya tienes sesión iniciada, puedes omitir --email/--password y el script
  intentará continuar sin login (pero es recomendable iniciar sesión).
"""

# ==========================
# Configuración y utilidades
# ==========================
ANIOS_MAX_ANTIGUEDAD = 4
DEFAULT_BASE_URL = "https://elpais.com/espana/"
DEFAULT_FILTRO = "elpais.com/espana/"



hostname = socket.gethostname()
if hostname == "R11":
    DEFAULT_DIR = "./rag_document_data/noticias/"
else:
    DEFAULT_DIR = "/mnt/disco6tb/Gover.Me/rag_document_data/noticias/"


def asegurar_symlink_rag(directorio_base: str) -> None:
    """Si existe /mnt/disco6tb, enlaza ./rag_document_data -> /mnt/disco6tb/Gover.Me/rag_document_data.
    No modifica nada si ya existe o si el directorio base no usa 'rag_document_data'.
    """
    try:
        base_norm = os.path.normpath(directorio_base)
        parts = base_norm.split(os.sep)
        if 'rag_document_data' not in parts:
            return
        idx = parts.index('rag_document_data')
        local_root = os.sep.join(parts[:idx + 1])
        if not os.path.isabs(local_root):
            local_root = os.path.join(os.getcwd(), local_root)
        mount_point = '/mnt/disco6tb'
        target_root = '/mnt/disco6tb/Gover.Me/rag_document_data'
        if os.path.ismount(mount_point) or os.path.isdir(mount_point):
            os.makedirs(target_root, exist_ok=True)
            if not os.path.exists(local_root):
                os.symlink(target_root, local_root)
    except Exception as e:
        print(f"⚠️ No se pudo asegurar enlace simbólico: {e}")


# =============================
# Login Selenium → cookies HTTP
# =============================

def selenium_login_and_get_session(email: str | None, password: str | None, start_url: str = "https://elpais.com/?ed=es") -> requests.Session:
    """Abre Selenium, acepta cookies, inicia sesión (si email/password disponibles),
    navega a España y devuelve un requests.Session con las cookies.
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')  # comenta si quieres ver el navegador
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1280, 1000)

    try:
        driver.get(start_url)

        # Aceptar cookies si aparece
        try:
            WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.ID, 'didomi-notice-agree-button'))
            ).click()
            time.sleep(1)
            print("🍪 Cookies aceptadas")
        except Exception:
            print("(i) Botón de cookies no encontrado – continuando…")

        # Si tenemos credenciales, intentar abrir modal/login y autenticar
        if email and password:
            # Los selectores pueden cambiar. Intentamos varios caminos comunes:
            logged = False
            try:
                # Botón de login (varía por despliegue/geografía)
                # 1) Ícono o enlace de acceso
                posibles_login = [
                    (By.CSS_SELECTOR, 'a[href*="login" i]'),
                    (By.CSS_SELECTOR, 'a[data-dtm-click*="login" i]'),
                    (By.XPATH, '//*[contains(@href, "login") or contains(text(), "Iniciar sesión") or contains(text(), "Acceder")]')
                ]
                for how, sel in posibles_login:
                    elems = driver.find_elements(how, sel)
                    if elems:
                        elems[0].click()
                        time.sleep(2)
                        break
            except Exception:
                pass

            # Intentar localizar campos de email/password (IDs pueden variar)
            try:
                # Algunos despliegues usan inputs con name="email" / name="password"
                email_input = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="email"], input[name="email"], #subsEmail'))
                )
                email_input.clear(); email_input.send_keys(email)

                pwd_input = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"], input[name="password"], #subsPassword'))
                )
                pwd_input.clear(); pwd_input.send_keys(password)

                # Botón de login (varias opciones)
                posibles_submit = [
                    (By.CSS_SELECTOR, 'button[type="submit"], #subsSignIn'),
                    (By.XPATH, '//button[contains(. ,"Iniciar") or contains(. ,"Acceder") or contains(. ,"Entrar")]')
                ]
                for how, sel in posibles_submit:
                    btns = driver.find_elements(how, sel)
                    if btns:
                        btns[0].click()
                        time.sleep(3)
                        break

                logged = True
                print("🔐 Login intentado (verifica en la página si aparece tu usuario)")
            except Exception:
                print("(i) No se pudo completar el flujo de login – quizá ya tienes sesión")

        # Navegar a España (punto de partida)
        driver.get(DEFAULT_BASE_URL)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(1.5)

        # Exportar cookies a requests.Session
        sess = requests.Session()
        for c in driver.get_cookies():
            sess.cookies.set(c['name'], c['value'], domain=c.get('domain'))
        # Headers base
        sess.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'
        })
        return sess

    finally:
        driver.quit()


# ======================
# Scraper recursivo EPES
# ======================
class ScraperRecursivoElPais:
    def __init__(self, session: requests.Session, url_base: str = DEFAULT_BASE_URL,
                 texto_filtro: str = DEFAULT_FILTRO, directorio_base: str = DEFAULT_DIR,
                 enlaces_por_nivel: int = 25, max_depth: int = 4):
        self.sess = session
        self.url_base = url_base
        self.texto_filtro = texto_filtro.lower()
        self.directorio_base = directorio_base
        self.enlaces_por_nivel = enlaces_por_nivel
        self.max_depth = max_depth

        self.urls_procesadas: set[str] = set()
        self.hashes_existentes: set[str] = set()

        asegurar_symlink_rag(self.directorio_base)
        os.makedirs(self.directorio_base, exist_ok=True)
        self._cargar_existentes()

    # --------- Estado previo ---------
    def _cargar_existentes(self) -> None:
        try:
            for fn in os.listdir(self.directorio_base):
                p = os.path.join(self.directorio_base, fn)
                if fn.endswith('.json'):
                    with open(p, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        u = data.get('url_original')
                        if u: self.urls_procesadas.add(u)
                        cc = (data.get('contenido_completo') or '').encode('utf-8')
                        if cc:
                            self.hashes_existentes.add(hashlib.md5(cc).hexdigest())
                elif fn.endswith('.json.gz'):
                    try:
                        with gzip.open(p, 'rt', encoding='utf-8') as f:
                            data = json.load(f)
                            u = data.get('url_original')
                            if u: self.urls_procesadas.add(u)
                            cc = (data.get('contenido_completo') or '').encode('utf-8')
                            if cc:
                                self.hashes_existentes.add(hashlib.md5(cc).hexdigest())
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error cargando existentes: {e}")

    # --------- Utilidades ---------
    @staticmethod
    def _nombre_archivo(url: str) -> str:
        h = hashlib.md5(url.encode()).hexdigest()[:8]
        ts = int(time.time())
        return f"elpais_{ts}_{h}.json.gz"

    @staticmethod
    def _es_reciente(iso: str | None) -> bool:
        try:
            if not iso:
                return True
            dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
            if dt.tzinfo is not None:
                dt = dt.astimezone().replace(tzinfo=None)
            umbral = datetime.now() - timedelta(days=ANIOS_MAX_ANTIGUEDAD * 365)
            return dt >= umbral
        except Exception:
            return True

    # --------- Rastreo ---------
    def obtener_enlaces_filtrados(self, url_base: str) -> list[str]:
        enlaces = []
        try:
            print(f"Extrayendo enlaces de: {url_base}")
            r = self.sess.get(url_base, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                url_abs = urljoin(url_base, href)
                if (self.texto_filtro in url_abs.lower()
                        and url_abs.startswith('http')
                        and url_abs not in self.urls_procesadas):
                    enlaces.append(url_abs)
            enlaces = list(set(enlaces))
            if len(enlaces) > self.enlaces_por_nivel:
                enlaces = random.sample(enlaces, self.enlaces_por_nivel)
            print(f"Encontrados {len(enlaces)} enlaces únicos con '{self.texto_filtro}'")
        except Exception as e:
            print(f"Error obteniendo enlaces: {e}")
        return enlaces

    # --------- Extracción ---------
    def extraer_noticia(self, url: str) -> dict | None:
        try:
            r = self.sess.get(url, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, 'html.parser')

            # Título: h1 o <meta property="og:title">
            titulo = None
            h1 = soup.find('h1')
            if h1:
                titulo = h1.get_text(strip=True)
            if not titulo:
                meta_og = soup.find('meta', attrs={'property': 'og:title'})
                if meta_og:
                    titulo = meta_og.get('content', '').strip()

            # Autor: meta name="author" o selectores comunes
            autor = None
            meta_author = soup.find('meta', attrs={'name': 'author'})
            if meta_author:
                autor = meta_author.get('content', '').strip()
            if not autor:
                by = soup.find(attrs={'itemprop': 'author'})
                if by:
                    autor = by.get_text(strip=True)

            # Fecha: meta article:published_time o <time datetime="">
            fecha_iso = None
            meta_time = soup.find('meta', attrs={'property': 'article:published_time'})
            if meta_time:
                fecha_iso = (meta_time.get('content') or '').strip()
            if not fecha_iso:
                time_el = soup.find('time')
                if time_el and time_el.get('datetime'):
                    fecha_iso = time_el.get('datetime').strip()

            # Subtítulo: <h2>, meta og:description
            subtitulo = None
            h2 = soup.find('h2')
            if h2:
                subtitulo = h2.get_text(strip=True)
            if not subtitulo:
                og_desc = soup.find('meta', attrs={'property': 'og:description'})
                if og_desc:
                    subtitulo = og_desc.get('content', '').strip()

            # Contenido: priorizar <article> p; si no, p dentro de main
            contenido = []
            article = soup.find('article')
            ps = []
            if article:
                ps = article.find_all('p')
            if not ps:
                main = soup.find('main')
                if main:
                    ps = main.find_all('p')
            if not ps:
                ps = soup.find_all('p')

            for p in ps:
                txt = p.get_text(" ", strip=True)
                if txt and len(txt) > 40:
                    contenido.append(txt)

            info = {
                'titulo': titulo,
                'subtitulo': subtitulo,
                'autor': autor,
                'fecha': fecha_iso,
                'fecha_formateada': None,
                'contenido': contenido,
                'contenido_completo': '\n\n'.join(contenido) if contenido else None,
                'url_original': url,
                'fecha_extraccion': datetime.now().isoformat(),
                'es_noticia_valida': bool(titulo and contenido),
                'diario_origen': 'El País',
            }
            return info
        except Exception as e:
            print(f"Error extrayendo {url}: {e}")
            return None

    # --------- Persistencia ---------
    def guardar_noticia(self, info: dict, profundidad: int) -> bool:
        try:
            info['profundidad'] = profundidad
            cc = (info.get('contenido_completo') or '').encode('utf-8')
            ch = hashlib.md5(cc).hexdigest()
            if ch in self.hashes_existentes:
                print("⚠️ Duplicado por hash – no se guarda")
                return False

            fn = self._nombre_archivo(info['url_original'])
            path = os.path.join(self.directorio_base, fn)
            with gzip.open(path, 'wt', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
            self.hashes_existentes.add(ch)
            print(f"✅ Guardado: {fn}")
            return True
        except Exception as e:
            print(f"❌ Error guardando: {e}")
            return False

    # --------- Motor recursivo ---------
    def procesar_nivel(self, urls: list[str], profundidad: int) -> list[str]:
        print(f"\n🔍 NIVEL {profundidad} — {len(urls)} URL(s)")
        siguientes: list[str] = []
        guardadas = 0

        for i, url in enumerate(urls, 1):
            if url in self.urls_procesadas and profundidad > 1:
                print(f"⏭️ Ya procesada: {url}")
                continue
            print(f"[{i}/{len(urls)}] {url}")

            info = self.extraer_noticia(url)
            if info and info['es_noticia_valida']:
                if not self._es_reciente(info.get('fecha')):
                    print(f"⏳ Descartada por antigüedad (> {ANIOS_MAX_ANTIGUEDAD} años)")
                else:
                    if self.guardar_noticia(info, profundidad):
                        guardadas += 1
                        self.urls_procesadas.add(url)
            else:
                print("⚠️ No válida o error en extracción")

            if profundidad < self.max_depth:
                nuevos = self.obtener_enlaces_filtrados(url)
                siguientes.extend(nuevos)

            time.sleep(0.6)

        print(f"📊 Guardadas: {guardadas} | Próximo nivel: {len(set(siguientes))}")
        if len(siguientes) > self.enlaces_por_nivel:
            siguientes = random.sample(siguientes, self.enlaces_por_nivel)

        return list(set(siguientes))

    def ejecutar(self, url_inicial: str | None = None) -> None:
        url0 = url_inicial or self.url_base
        print("🚀 INICIO SCRAPING El País – España")
        print("=" * 60)
        print(f"URL inicial: {url0}")
        print(f"Filtro: '{self.texto_filtro}'")
        print(f"Profundidad: {self.max_depth} | Enlaces por nivel: {self.enlaces_por_nivel}")
        print(f"Directorio: {self.directorio_base}")
        print("=" * 60)

        while True:
            actuales = [url0]
            for profundidad in range(1, self.max_depth + 1):
                if not actuales:
                    print("❌ Sin URLs para procesar – fin")
                    break
                actuales = self.procesar_nivel(actuales, profundidad) if (actuales := self.procesar_nivel(actuales, profundidad)) else []

        print("\n🎉 FINALIZADO")


# ===============
# CLI principal
# ===============

def main():
    parser = argparse.ArgumentParser(
        description="Scraper recursivo de El País (España) con login y guardado JSON.gz",
    )
    # Todos los argumentos son opcionales; con cero flags funciona con defaults/ENV.
    parser.add_argument('--email', default=os.getenv('ELPAIS_EMAIL', 'maximiliano.lucius@gmail.com'), help='Email de suscripción (opcional)')
    parser.add_argument('--password', default=os.getenv('ELPAIS_PASSWORD', 'diganDar.73'), help='Password de suscripción (opcional)')
    parser.add_argument('--url', default=DEFAULT_BASE_URL, help='URL base (default: %(default)s)')
    parser.add_argument('--filtro', default=DEFAULT_FILTRO, help='Filtro de enlaces (default: %(default)s)')
    parser.add_argument('--directorio', default=DEFAULT_DIR, help='Directorio destino (default: %(default)s)')
    parser.add_argument('--enlaces', type=int, default=250, help='Máx. enlaces por nivel (default: %(default)d)')
    parser.add_argument('--depth', type=int, default=4, help='Profundidad máxima (default: %(default)d)')

    args = parser.parse_args()


    # 1) Login Selenium y sesión HTTP con cookies
    session = selenium_login_and_get_session(args.email, args.password)

    # 2) Scraper recursivo
    scraper = ScraperRecursivoElPais(
        session=session,
        url_base=args.url,
        texto_filtro=args.filtro,
        directorio_base=args.directorio,
        enlaces_por_nivel=args.enlaces,
        max_depth=args.depth,
    )
    scraper.ejecutar(args.url)


if __name__ == '__main__':
    main()
