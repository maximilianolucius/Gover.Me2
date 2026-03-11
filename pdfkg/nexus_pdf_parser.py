"""
Parser de archivos PDF de datos de turismo de Andalucía.
Extrae tablas de los PDFs y las convierte a formato estructurado.
"""

import os
import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import pdfplumber

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mapeo de meses en español
MESES_MAP = {
    "ene": 1, "enero": 1,
    "feb": 2, "febrero": 2,
    "mar": 3, "marzo": 3,
    "abr": 4, "abril": 4,
    "may": 5, "mayo": 5,
    "jun": 6, "junio": 6,
    "jul": 7, "julio": 7,
    "ago": 8, "agosto": 8,
    "sep": 9, "septiembre": 9,
    "oct": 10, "octubre": 10,
    "nov": 11, "noviembre": 11,
    "dic": 12, "diciembre": 12
}

# Mapeo de categorías según el título de la página
CATEGORIA_MAP = {
    "TOTAL TURISMO": "total_turistas",
    "TURISMO ESPAÑOL": "espanoles",
    "TURISMO ANDALUZ": "andaluces",
    "RESTO DE ESPAÑA": "resto_espana",
    "TURISMO EXTRANJERO": "extranjeros",
    "TURISMO BRITÁNICO": "britanicos",
    "TURISMO ALEMÁN": "alemanes",
    "OTROS MERCADOS": "otros_mercados",
    "LITORAL": "litoral",
    "INTERIOR": "interior",
    "CRUCEROS": "cruceros",
    "CIUDAD": "ciudad",
    "CULTURAL": "cultural",
    "ALMERÍA": "almeria",
    "CÁDIZ": "cadiz",
    "CÓRDOBA": "cordoba",
    "GRANADA": "granada",
    "HUELVA": "huelva",
    "JAÉN": "jaen",
    "MÁLAGA": "malaga",
    "SEVILLA": "sevilla"
}


def parse_pdf_filename(filename: str) -> Optional[Dict[str, Any]]:
    """
    Parsea el nombre de archivo PDF para extraer metadata.

    Args:
        filename: Nombre del archivo (ej: "ultimos-datos_ene25.pdf")

    Returns:
        Dict con metadata o None si el formato no es válido
    """
    try:
        # Remover extensión
        base_name = filename.replace(".pdf", "").replace("_2", "")

        # Pattern: ultimos-datos_{mes}{año}
        pattern = r"ultimos-datos_([a-z]{3})(\d{2})$"
        match = re.match(pattern, base_name)

        if not match:
            logger.warning(f"Nombre de archivo PDF no coincide con el patrón: {filename}")
            return None

        mes_str, anio_str = match.groups()

        # Validar y convertir mes
        mes = MESES_MAP.get(mes_str.lower())
        if mes is None:
            logger.warning(f"Mes no reconocido: {mes_str}")
            return None

        # Convertir año (25 -> 2025, 24 -> 2024, etc.)
        anio = 2000 + int(anio_str)

        return {
            "mes": mes,
            "mes_str": mes_str,
            "anio": anio,
            "archivo_original": filename
        }

    except Exception as e:
        logger.error(f"Error al parsear nombre de archivo PDF '{filename}': {e}")
        return None


def identify_category_from_title(title: str) -> Optional[str]:
    """
    Identifica la categoría basándose en el título de la página.

    Args:
        title: Título de la página (ej: "TOTAL TURISMO EN ANDALUCÍA")

    Returns:
        Categoría identificada o None
    """
    title_upper = title.upper()

    for key, category in CATEGORIA_MAP.items():
        if key in title_upper:
            return category

    return None


def parse_metric_value(value_str: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Parsea un string de valor que puede contener número y variación.

    Args:
        value_str: String como "1.847.648 -0,3% abril - 2024"

    Returns:
        Tupla (valor, variacion_interanual)
    """
    if not value_str or value_str.strip() == '':
        return None, None

    try:
        # Extraer número principal (puede tener puntos de miles y coma decimal)
        # Ejemplo: "1.847.648" o "0,3%"
        number_match = re.search(r'([\d.]+(?:,\d+)?)', value_str)
        valor = None
        if number_match:
            num_str = number_match.group(1)
            # Convertir formato español: quitar puntos de miles, reemplazar coma por punto
            num_str = num_str.replace('.', '').replace(',', '.')
            valor = float(num_str)

        # Extraer variación interanual (puede ser positivo o negativo)
        # Ejemplo: "-0,3%" o "+5,2%"
        var_match = re.search(r'([+-]?[\d,]+)%', value_str)
        variacion = None
        if var_match:
            var_str = var_match.group(1).replace(',', '.')
            variacion = float(var_str)

        return valor, variacion

    except Exception as e:
        logger.warning(f"Error al parsear valor '{value_str}': {e}")
        return None, None


def extract_metrics_from_pdf_table(table: List[List], category: str, metadata: Dict) -> List[Dict[str, Any]]:
    """
    Extrae métricas de una tabla de PDF.

    Args:
        table: Lista de filas de la tabla
        category: Categoría de los datos
        metadata: Metadata del archivo PDF

    Returns:
        Lista de métricas extraídas
    """
    metrics = []

    # Encontrar índices de columnas para DATOS MENSUALES, ACUMULADO, AÑO COMPLETO
    header_row_idx = None
    for i, row in enumerate(table):
        if any(cell and 'DATOS MENSUALES' in str(cell) for cell in row):
            header_row_idx = i
            break

    if header_row_idx is None:
        return metrics

    # Procesar filas de datos
    for row_idx in range(header_row_idx + 2, len(table)):  # +2 para saltar headers
        row = table[row_idx]

        if not row or len(row) < 2:
            continue

        metrica_nombre = row[0]
        if not metrica_nombre or metrica_nombre.strip() == '':
            continue

        metrica_nombre = fix_encoding(metrica_nombre.strip())

        # Procesar las tres columnas: mensual, acumulado, anual
        columnas = {
            "mensual": (1, "mensual"),
            "acumulado": (4, "acumulado"),
            "anual": (6, "anual")
        }

        for col_name, (col_idx, periodo_tipo) in columnas.items():
            if col_idx >= len(row):
                continue

            valor_str = row[col_idx]
            if not valor_str:
                continue

            metrica_valor, variacion_interanual = parse_metric_value(str(valor_str))

            if metrica_valor is not None:
                metric_doc = {
                    "categoria": category,
                    "mes": metadata["mes"],
                    "mes_str": metadata["mes_str"],
                    "anio": metadata["anio"],
                    "periodo_tipo": periodo_tipo,
                    "metrica_nombre": metrica_nombre,
                    "metrica_valor": metrica_valor,
                    "variacion_interanual": variacion_interanual,
                    "periodo_descripcion": f"{metadata['mes_str']} - {metadata['anio']}",
                    "fuente_archivo": metadata["archivo_original"],
                    "es_limpio": False,
                    "fuente_tipo": "pdf",
                    "timestamp_ingestion": datetime.now().isoformat(),
                }

                # Agregar provincia si la categoría es una provincia
                provincias = ["almeria", "cadiz", "cordoba", "granada", "huelva", "jaen", "malaga", "sevilla"]
                if category in provincias:
                    metric_doc["provincia"] = category.capitalize()

                metrics.append(metric_doc)

    return metrics


def fix_encoding(text: str) -> str:
    """
    Arregla problemas de encoding comunes en PDFs.

    Args:
        text: Texto con problemas de encoding

    Returns:
        Texto corregido
    """
    if not text:
        return text

    # Mapeo de caracteres mal codificados
    replacements = {
        'N(cid:184)mero': 'Número',
        'n(cid:184)mero': 'número',
        'Andaluc(cid:171)a': 'Andalucía',
        'andaluc(cid:171)a': 'andalucía',
        'Almer(cid:171)a': 'Almería',
        'almer(cid:171)a': 'almería',
        'M(cid:159)laga': 'Málaga',
        'm(cid:159)laga': 'málaga',
        'C(cid:159)diz': 'Cádiz',
        'c(cid:159)diz': 'cádiz',
        'C(cid:162)rdoba': 'Córdoba',
        'c(cid:162)rdoba': 'córdoba',
        'Jan': 'Jaén',
        'jan': 'jaén',
        'Espa(cid:175)a': 'España',
        'espa(cid:175)a': 'españa',
        'Bah(cid:171)a': 'Bahía',
        'bah(cid:171)a': 'bahía',
        # Caracteres especiales comunes
        '(cid:159)': 'á',
        '(cid:171)': 'í',
        '(cid:162)': 'ó',
        '(cid:184)': 'ú',
        '(cid:175)': 'ñ',
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    return text


def extract_metrics_from_pdf(file_path: str, metadata: Dict) -> List[Dict[str, Any]]:
    """
    Extrae todas las métricas de un archivo PDF.

    Args:
        file_path: Ruta al archivo PDF
        metadata: Metadata extraída del nombre del archivo

    Returns:
        Lista de métricas extraídas
    """
    all_metrics = []

    try:
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Extraer texto para identificar categoría
                text = page.extract_text()
                if not text:
                    continue

                # Arreglar encoding del texto
                text = fix_encoding(text)

                # Identificar categoría de la página
                lines = text.split('\n')
                category = None
                for line in lines[:5]:  # Buscar en las primeras líneas
                    cat = identify_category_from_title(line)
                    if cat:
                        category = cat
                        break

                if not category:
                    continue

                # Extraer tablas de la página
                tables = page.extract_tables()

                for table in tables:
                    if len(table) > 5:  # Debe tener suficientes filas
                        metrics = extract_metrics_from_pdf_table(table, category, metadata)
                        all_metrics.extend(metrics)

        logger.info(f"Extraídas {len(all_metrics)} métricas de {file_path}")

    except Exception as e:
        logger.error(f"Error al procesar PDF {file_path}: {e}")

    return all_metrics


if __name__ == "__main__":
    # Test del parser
    import sys

    if len(sys.argv) > 1:
        pdf_file = sys.argv[1]
    else:
        pdf_file = "nexus/ultimos-datos_abr24.pdf"

    print(f"\nProbando parser con: {pdf_file}")
    print("=" * 80)

    # Parsear nombre de archivo
    metadata = parse_pdf_filename(os.path.basename(pdf_file))
    if not metadata:
        print("❌ Error al parsear nombre de archivo")
        sys.exit(1)

    print(f"✅ Metadata: {metadata}")
    print()

    # Extraer métricas
    metrics = extract_metrics_from_pdf(pdf_file, metadata)

    print(f"\n📊 Total métricas extraídas: {len(metrics)}")

    if metrics:
        print("\n🔍 Primeras 5 métricas:")
        print("-" * 80)
        for i, metric in enumerate(metrics[:5], 1):
            print(f"\n{i}. {metric['categoria']} - {metric['metrica_nombre']}")
            print(f"   Valor: {metric['metrica_valor']}")
            print(f"   Variación: {metric['variacion_interanual']}%")
            print(f"   Periodo: {metric['periodo_tipo']}")
