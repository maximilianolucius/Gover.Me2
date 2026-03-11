#!/usr/bin/env python3
import threading
import subprocess
import logging
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_symlink_to_external_storage():
    """Asegurar enlace simbólico al almacenamiento externo"""
    data_dir = "./rag_document_data/noticias/"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)
        logging.info(f"📁 Directorio creado: {data_dir}")

def run_single_scraper(scraper_cmd, scraper_id):
    """Ejecutar un scraper individual"""
    try:
        logging.info(f"🔄 Iniciando scraper #{scraper_id}")
        result = subprocess.run(scraper_cmd, capture_output=True, text=True, check=True)
        logging.info(f"✅ Scraper #{scraper_id} completado exitosamente")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Error en scraper #{scraper_id}: {e}")
        logging.error(f"Salida de error: {e.stderr}")
    except Exception as e:
        logging.error(f"❌ Error inesperado en scraper #{scraper_id}: {e}")

def run_scrapers():
    """Ejecutar los scrapers de datos en paralelo"""
    ensure_symlink_to_external_storage()
    scrapers = [
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://www.abc.es/sevilla/',
         '--filtro', 'sevilla', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://www.diariosur.es/',
         '--filtro', 'diariosur.es', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://www.diariodesevilla.es',
         '--filtro', 'diariodesevilla.es', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://www.elcorreoweb.es/andalucia/',
         '--filtro', 'elcorreoweb.es/andalucia/', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://www.eldiario.es/andalucia/',
         '--filtro', 'eldiario.es/andalucia/', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://www.ideal.es',
         '--filtro', 'www.ideal.es', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
        ['python', 'rag_document_tools/scraper_recursivo_diarios.py', '--url', 'https://elpais.com/?ed=es',
         '--filtro', 'elpais.com/espana/', '--directorio', './rag_document_data/noticias/',
         '--enlaces', '2000', '--depth', '30'],
    ]

    logging.info(f"🚀 Iniciando {len(scrapers)} scrapers en paralelo...")

    threads = []
    for i, scraper_cmd in enumerate(scrapers, 1):
        thread = threading.Thread(
            target=run_single_scraper,
            args=(scraper_cmd, i)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    logging.info("🏁 Todos los scrapers han terminado")

if __name__ == "__main__":
    run_scrapers()