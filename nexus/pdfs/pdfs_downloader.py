import json
import os
import re
from pathlib import Path
from collections import Counter
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

DOWNLOAD_DIR = Path("./nexus/data/ultimos_datos_turisticos")


def extract_pdf_links(url: str, timeout: int = 20) -> list[str]:
    """Devuelve una lista (ordenada, sin duplicados) de URLs de PDFs en la página."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Error al obtener la página: {e}")
        return []

    soup = BeautifulSoup(r.content, "html.parser")
    links = set()

    # <a href="...pdf">
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            links.add(urljoin(url, href))

    # Cualquier atributo que termine en .pdf (data-*, src, etc.)
    for tag in soup.find_all(True):
        for val in tag.attrs.values():
            if isinstance(val, str) and val.lower().endswith(".pdf"):
                links.add(urljoin(url, val))

    return sorted(links)


def save_txt(links: list[str], path: str = "pdf_links.txt") -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(links))
    print(f"Enlaces guardados en: {path}")


def save_json(links: list[str], source_url: str, path: str = "pdf_links.json") -> None:
    payload = {"source_url": source_url, "total_pdfs": len(links), "pdf_links": links}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Enlaces guardados en JSON: {path}")


# ------------------------ Descarga ------------------------ #

_filename_token_re = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_filename(name: str) -> str:
    # Reemplaza espacios y caracteres raros, evita nombres vacíos
    name = unquote(name).strip().replace(" ", "_")
    name = _filename_token_re.sub("_", name)
    return name or "archivo.pdf"


def _name_from_cd(cd: str | None) -> str | None:
    if not cd:
        return None
    # filename*=UTF-8''name.pdf  (RFC 5987)
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.I)
    if m:
        return _sanitize_filename(m.group(1))
    # filename="name.pdf" o filename=name.pdf
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, flags=re.I)
    if m:
        return _sanitize_filename(m.group(1))
    return None


def _filename_for(url: str, resp: requests.Response) -> str:
    # 1) Content-Disposition
    cd_name = _name_from_cd(resp.headers.get("Content-Disposition"))
    if cd_name:
        base = cd_name
    else:
        # 2) Último segmento de la URL sin query/fragment
        path = urlparse(url)._replace(query="", fragment="").path
        base = os.path.basename(path) or "archivo.pdf"
        base = _sanitize_filename(base)
    # Asegurar extensión .pdf
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    return base


def _unique_path(dirpath: Path, filename: str) -> Path:
    p = dirpath / filename
    if not p.exists():
        return p
    stem, ext = os.path.splitext(filename)
    i = 2
    while True:
        candidate = dirpath / f"{stem}_{i}{ext}"
        if not candidate.exists():
            return candidate
        i += 1


def download_pdf(url: str, dest_dir: Path, timeout: int = 60, chunk: int = 1 << 20) -> Path | None:
    try:
        with requests.get(url, headers=HEADERS, timeout=timeout, stream=True) as r:
            r.raise_for_status()
            fname = _filename_for(url, r)
            dest_dir.mkdir(parents=True, exist_ok=True)
            out_path = _unique_path(dest_dir, fname)

            # Validar (opcional) por Content-Type, pero no bloquear si falta
            ctype = r.headers.get("Content-Type", "").lower()
            if "pdf" not in ctype and not url.lower().endswith(".pdf"):
                print(f"⚠️  Content-Type no parece PDF ({ctype}) para: {url}")

            total = int(r.headers.get("Content-Length", "0") or 0)
            downloaded = 0

            with open(out_path, "wb") as f:
                for chunk_bytes in r.iter_content(chunk_size=chunk):
                    if chunk_bytes:
                        f.write(chunk_bytes)
                        downloaded += len(chunk_bytes)

            # Tamaño final
            size_mb = downloaded / (1024 * 1024)
            print(f"⬇️  Guardado: {out_path} ({size_mb:.2f} MB)")
            return out_path

    except requests.RequestException as e:
        print(f"❌ Error descargando {url}: {e}")
    except OSError as e:
        print(f"❌ Error de escritura para {url}: {e}")
    return None


def download_all(links: list[str], dest_dir: Path) -> list[Path]:
    results = []
    for i, link in enumerate(links, 1):
        print(f"[{i}/{len(links)}] Descargando: {link}")
        p = download_pdf(link, dest_dir)
        if p:
            results.append(p)
    return results


# ------------------------ Resumen ------------------------ #

def summarize(links: list[str]) -> None:
    print("\n" + "=" * 60)
    print(f"RESUMEN: Se encontraron {len(links)} archivos PDF")
    print("=" * 60)

    if not links:
        print(
            "\nNo se encontraron PDFs. Posibles causas:\n"
            "  - Contenido cargado dinámicamente por JavaScript\n"
            "  - PDFs en rutas no enlazadas\n"
            "  - Autenticación requerida"
        )
        return

    print("\nListado completo de PDFs encontrados:")
    print("-" * 40)
    for i, u in enumerate(links, 1):
        print(f"{i}. {u}")

    dom_count = Counter(urlparse(u).netloc for u in links)
    print("\nPDFs por dominio:")
    for dom, cnt in dom_count.items():
        print(f"  - {dom}: {cnt} PDFs")


# ------------------------ Main ------------------------ #

if __name__ == "__main__":
    URL = "https://nexus.andalucia.org/es/data/informes/ultimos-datos-turisticos/"

    print("=" * 60)
    print("EXTRACTOR Y DESCARGA DE PDFs - NEXUS ANDALUCÍA")
    print("=" * 60 + "\n")

    pdfs = extract_pdf_links(URL)
    summarize(pdfs)

    if pdfs:
        save_txt(pdfs, "pdf_links.txt")
        save_json(pdfs, URL, "pdf_links.json")

        print("\n" + "=" * 60)
        print(f"Descargando en: {DOWNLOAD_DIR.resolve()}")
        print("=" * 60)
        saved = download_all(pdfs, DOWNLOAD_DIR)
        print(f"\n✅ Descargados: {len(saved)}/{len(pdfs)} archivos en {DOWNLOAD_DIR.resolve()}")

    print("\n" + "=" * 60)
    print("Dependencias: pip install requests beautifulsoup4")
    print("=" * 60)
