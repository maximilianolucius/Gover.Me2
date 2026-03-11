"""
Sistema de consultas Text-to-SQL para análisis de datos de turismo.
Convierte preguntas en lenguaje natural a consultas AQL y genera respuestas.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dotenv import load_dotenv
VALID_CATEGORIES = {
    "total_turistas",
    "espanoles",
    "andaluces",
    "resto_espana",
    "extranjeros",
    "britanicos",
    "alemanes",
    "otros_mercados",
    "litoral",
    "interior",
    "cruceros",
    "ciudad",
    "cultural",
    "almeria",
    "cadiz",
    "cordoba",
    "granada",
    "huelva",
    "jaen",
    "malaga",
    "sevilla",
}



try:  # Dependencia opcional: google-generativeai
    import google.generativeai as genai  # type: ignore[attr-defined]
except ModuleNotFoundError as exc:  # pragma: no cover - se maneja en tiempo de ejecución
    genai = None  # type: ignore[assignment]
    _GENAI_IMPORT_ERROR = exc
else:
    _GENAI_IMPORT_ERROR = None

try:  # Compatibilidad tanto como paquete como script directo
    if __package__:
        from .nexus_db import NexusDB, initialize_nexus_db  # type: ignore[attr-defined]
    else:  # pragma: no cover - ruta usada al ejecutar como script
        from nexus_db import NexusDB, initialize_nexus_db  # type: ignore[attr-defined]
except ImportError as exc:  # pragma: no cover - error de importación claro
    raise ImportError(
        "No se pudo importar las dependencias internas de Nexus. "
        "Ejecuta este módulo dentro del paquete 'pdfkg' o añade la carpeta al PYTHONPATH."
    ) from exc

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Configurar Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-exp")

if genai is not None and GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


class NexusQueryEngine:
    """
    Motor de consultas que convierte preguntas en lenguaje natural
    a queries AQL y genera respuestas contextualizadas.
    """

    def __init__(self, db: Optional[NexusDB] = None):
        """
        Inicializa el motor de consultas.

        Args:
            db: Instancia de NexusDB (si None, se crea una nueva)
        """
        if genai is None:
            hint = (
                "Instala la dependencia opcional 'google-generativeai>=0.3.0' y define GEMINI_API_KEY "
                "para habilitar las consultas Nexus."
            )
            raise RuntimeError(hint) from _GENAI_IMPORT_ERROR

        self.db = db if db else initialize_nexus_db()

        if self.db is None:
            raise RuntimeError("No se pudo inicializar la base de datos Nexus. Verifica la conexión a ArangoDB.")
        self.model = genai.GenerativeModel(GEMINI_MODEL)

        # Información de schema para el LLM
        self.schema_info = """
        Base de datos: ArangoDB
        Colección: metricas_turismo

        Campos disponibles:
        - categoria: string (total_turistas, espanoles, britanicos, cruceros, malaga, etc.)
        - mes: int (1-12)
        - mes_str: string (ene, feb, mar, etc.)
        - anio: int (2023, 2024, 2025)
        - periodo_tipo: string (mensual, acumulado, anual)
        - metrica_nombre: string (nombre de la métrica)
        - metrica_valor: float (valor numérico)
        - variacion_interanual: float (% de variación)
        - provincia: string (solo para categorías de provincias)
        - fuente_archivo: string (archivo origen)

        Métricas comunes:
        - "Número de viajeros en establecimientos hoteleros"
        - "Número de pernoctaciones en establecimientos hoteleros"
        - "Cuota (% sobre total pernoctaciones en España)"
        - "Llegadas de pasajeros a aeropuertos andaluces"
        - "Número de turistas (millones)"
        - "Estancia Media (número de días)"
        - "Gasto medio diario (euros)"

        Categorías:
        - total_turistas, espanoles, andaluces, resto_espana, extranjeros
        - britanicos, alemanes, otros_mercados
        - litoral, interior, cruceros, ciudad, cultural
        - almeria, cadiz, cordoba, granada, huelva, jaen, malaga, sevilla

        Periodos:
        - mensual: datos de un mes específico
        - acumulado: datos acumulados del año hasta ese mes
        - anual: datos del año completo
        """

    def classify_question(self, question: str) -> str:
        """
        Clasifica una pregunta según el tipo de respuesta que requiere.

        Args:
            question: Pregunta del usuario

        Returns:
            str: 'sql' (datos estructurados), 'rag' (info cualitativa), 'hybrid'
        """
        classification_prompt = f"""
Clasifica la siguiente pregunta sobre turismo en Andalucía:

Pregunta: "{question}"

Tipos de pregunta:
- "sql": Pregunta que requiere datos numéricos de la base de datos (métricas, comparaciones, agregaciones)
- "rag": Pregunta cualitativa sobre tendencias, análisis narrativo, gráficos en PDFs
- "hybrid": Pregunta que requiere ambos tipos de información

Responde SOLO con una de estas palabras: sql, rag, hybrid
"""

        try:
            response = self.model.generate_content(classification_prompt)
            classification = response.text.strip().lower()

            if classification in ["sql", "rag", "hybrid"]:
                logger.info(f"Pregunta clasificada como: {classification}")
                return classification
            else:
                logger.warning(f"Clasificación inesperada: {classification}, usando 'sql' por defecto")
                return "sql"

        except Exception as e:
            logger.error(f"Error al clasificar pregunta: {e}")
            return "sql"  # Default a SQL

    def extract_query_parameters(self, question: str) -> Dict[str, Any]:
        """
        Extrae parámetros estructurados de una pregunta en lenguaje natural.

        Args:
            question: Pregunta del usuario

        Returns:
            Dict con parámetros de la consulta
        """
        extraction_prompt = f"""
Analiza la siguiente pregunta sobre turismo en Andalucía y extrae los parámetros de consulta.

Pregunta: "{question}"

{self.schema_info}

Extrae la siguiente información en formato JSON:
{{
    "categorias": ["lista de categorías mencionadas"],
    "provincias": ["lista de provincias mencionadas"],
    "meses": [lista de números de mes 1-12],
    "anios": [lista de años],
    "periodo_tipo": "mensual/acumulado/anual o null",
    "metricas": ["lista de nombres de métricas mencionadas"],
    "operacion": "comparar/agregar/filtrar/tendencia",
    "filtros_adicionales": {{"cualquier otro filtro"}}
}}

IMPORTANTE:
- Si se menciona "primer trimestre" o "Q1", incluir meses [1, 2, 3]
- Si se menciona "segundo trimestre" o "Q2", incluir meses [4, 5, 6]
- Si se menciona "verano", incluir meses [6, 7, 8]
- Si se menciona "invierno", incluir meses [12, 1, 2]
- Las categorías deben usar los nombres exactos del schema (ej: "britanicos", no "británicos")
- Las provincias deben estar en minúsculas
- Para comparaciones entre años completos (ej: "2022 vs 2023"), usar periodo_tipo: "acumulado" (NO "anual")
- El dataset solo tiene tres periodo_tipo: "mensual", "acumulado", "anual" (pero "anual" rara vez existe, usar "acumulado" para datos anuales)

Responde SOLO con el JSON, sin texto adicional.
"""

        try:
            response = self.model.generate_content(extraction_prompt)
            json_text = response.text.strip()

            # Limpiar markdown si está presente
            if json_text.startswith("```json"):
                json_text = json_text.replace("```json", "").replace("```", "").strip()
            elif json_text.startswith("```"):
                json_text = json_text.replace("```", "").strip()

            params = json.loads(json_text)
            logger.info(f"Parámetros extraídos: {params}")
            return params

        except Exception as e:
            logger.error(f"Error al extraer parámetros: {e}")
            return {
                "categorias": [],
                "provincias": [],
                "meses": [],
                "anios": [],
                "periodo_tipo": None,
                "metricas": [],
                "operacion": "filtrar",
                "filtros_adicionales": {}
            }

    def build_aql_query(self, params: Dict[str, Any]) -> Tuple[str, Dict]:
        """
        Construye una consulta AQL basada en parámetros extraídos.

        Args:
            params: Parámetros de la consulta

        Returns:
            Tupla (query_aql, bind_vars)
        """
        # Construir condiciones WHERE
        conditions = []
        bind_vars = {}

        categorias = params.get("categorias") or []
        if categorias:
            normalized = [cat for cat in categorias if cat in VALID_CATEGORIES]
            if len(normalized) < len(categorias):
                normalized.append("otros_mercados")
            if "otros_mercados" in normalized and "britanicos" not in normalized:
                normalized.append("britanicos")
            if "otros_mercados" in normalized and "alemanes" not in normalized:
                normalized.append("alemanes")
            if normalized:
                # Eliminar duplicados preservando el orden
                params["categorias"] = list(dict.fromkeys(normalized))
            else:
                params["categorias"] = ["otros_mercados"]

        # Filtro por categorías
        if params.get("categorias"):
            conditions.append("doc.categoria IN @categorias")
            bind_vars["categorias"] = params["categorias"]

        # Filtro por provincias (subset de categorías)
        if params.get("provincias"):
            conditions.append("doc.provincia IN @provincias")
            bind_vars["provincias"] = [p.capitalize() for p in params["provincias"]]

        # Filtro por meses
        if params.get("meses"):
            conditions.append("doc.mes IN @meses")
            bind_vars["meses"] = params["meses"]

        # Filtro por años
        if params.get("anios"):
            conditions.append("doc.anio IN @anios")
            bind_vars["anios"] = params["anios"]

        # Filtro por tipo de periodo
        if params.get("periodo_tipo"):
            periodo = params["periodo_tipo"]
            # Fallback: 'anual' no existe en datos, usar 'acumulado'
            if periodo == "anual":
                periodo = "acumulado"
            conditions.append("doc.periodo_tipo == @periodo_tipo")
            bind_vars["periodo_tipo"] = periodo

        # Filtro por métricas
        if params.get("metricas"):
            categorias_actuales = params.get("categorias", [])
            metricas_ajustadas = []
            for metrica in params["metricas"]:
                metrica_lower = metrica.lower()
                synonyms = [metrica_lower]

                if "cruceros" in categorias_actuales and ("turistas" in metrica_lower or "turismo" in metrica_lower):
                    synonyms.append("pasajeros")

                if any(cat in categorias_actuales for cat in ("otros_mercados", "britanicos", "alemanes", "extranjeros")) and (
                    "turistas" in metrica_lower
                    or "turismo" in metrica_lower
                    or "viajeros" in metrica_lower
                ):
                    synonyms.extend([
                        "número de viajeros en establecimientos hoteleros",
                        "numero de viajeros en establecimientos hoteleros",
                        "viajeros en establecimientos hoteleros",
                        "viajeros"
                    ])

                for synonym in synonyms:
                    if synonym not in metricas_ajustadas:
                        metricas_ajustadas.append(synonym)

            # Búsqueda flexible por nombre de métrica
            metric_conditions = []
            for i, metrica in enumerate(metricas_ajustadas):
                metric_var = f"metrica{i}"
                metric_conditions.append(f"CONTAINS(LOWER(doc.metrica_nombre), @{metric_var})")
                bind_vars[metric_var] = metrica
            if metric_conditions:
                conditions.append(f"({' OR '.join(metric_conditions)})")

        # Construir WHERE clause
        where_clause = " AND ".join(conditions) if conditions else "true"

        # Determinar tipo de operación
        operacion = params.get("operacion", "filtrar")

        if operacion == "agregar":
            # Query con agregación
            aql = f"""
            FOR doc IN metricas_turismo
                FILTER {where_clause}
                COLLECT
                    categoria = doc.categoria,
                    anio = doc.anio,
                    metrica = doc.metrica_nombre
                AGGREGATE
                    total = SUM(doc.metrica_valor),
                    promedio = AVG(doc.metrica_valor),
                    count = COUNT(doc)
                SORT anio DESC, categoria
                LIMIT 100
                RETURN {{
                    categoria: categoria,
                    anio: anio,
                    metrica: metrica,
                    total: total,
                    promedio: promedio,
                    registros: count
                }}
            """
        else:
            # Query simple con resultados
            aql = f"""
            FOR doc IN metricas_turismo
                FILTER {where_clause}
                SORT doc.anio DESC, doc.mes DESC
                LIMIT 100
                RETURN {{
                    categoria: doc.categoria,
                    mes: doc.mes,
                    mes_str: doc.mes_str,
                    anio: doc.anio,
                    periodo_tipo: doc.periodo_tipo,
                    metrica_nombre: doc.metrica_nombre,
                    metrica_valor: doc.metrica_valor,
                    variacion_interanual: doc.variacion_interanual,
                    fuente: doc.fuente_archivo,
                    provincia: doc.provincia
                }}
            """

        logger.info(f"Query AQL generada: {aql}")
        logger.info(f"Bind vars: {bind_vars}")

        return aql, bind_vars

    def execute_query(self, aql_query: str, bind_vars: Dict) -> List[Dict]:
        """
        Ejecuta una consulta AQL y retorna los resultados.

        Args:
            aql_query: Consulta AQL
            bind_vars: Variables de binding

        Returns:
            Lista de resultados
        """
        try:
            results = self.db.query_metrics(aql_query, bind_vars)
            logger.info(f"Query ejecutada: {len(results)} resultados")
            return results
        except Exception as e:
            logger.error(f"Error al ejecutar query: {e}")
            return []

    def format_answer(self, question: str, results: List[Dict], params: Dict) -> str:
        """
        Formatea los resultados de la consulta en una respuesta en lenguaje natural.

        Args:
            question: Pregunta original del usuario
            results: Resultados de la consulta
            params: Parámetros de la consulta

        Returns:
            str: Respuesta formateada
        """
        if not results:
            return "No tengo datos suficientes para responder a tu pregunta. Por favor, intenta reformularla o verifica que los datos estén disponibles."

        # Preparar contexto para el LLM
        results_summary = json.dumps(results[:20], ensure_ascii=False, indent=2)  # Limitar a 20 para no sobrepasar token limit

        numeric_entries = []
        for item in results:
            value = item.get("metrica_valor")
            if isinstance(value, (int, float)):
                numeric_entries.append({
                    "categoria": item.get("categoria"),
                    "metrica": item.get("metrica_nombre"),
                    "valor": value,
                    "periodo": item.get("periodo_descripcion"),
                })

        numeric_entries.sort(key=lambda x: x["valor"], reverse=True)
        top_entry = numeric_entries[0] if numeric_entries else None
        top_lines = []
        for entry in numeric_entries[:10]:
            top_lines.append(f"- {entry['metrica']} => {entry['valor']:,}")

        from collections import defaultdict

        year_category_totals = defaultdict(lambda: defaultdict(float))
        for item in results:
            value = item.get("metrica_valor")
            if isinstance(value, (int, float)):
                year = item.get("anio")
                category = item.get("categoria") or "desconocido"
                year_category_totals[year][category] += float(value)

        year_totals = {year: sum(cat_totals.values()) for year, cat_totals in year_category_totals.items()}
        comparison_lines = []
        for year in sorted(year_totals.keys()):
            comparison_lines.append(f"Año {year}: {year_totals[year]:,.0f} viajeros totales")

        coverage_lines = []
        requested_categories = set(params.get("categorias") or [])
        if requested_categories:
            for year, cat_totals in year_category_totals.items():
                missing = requested_categories - set(cat_totals.keys())
                if missing:
                    coverage_lines.append(
                        f"Cobertura {year}: sin datos para {', '.join(sorted(missing))}"
                    )

        if len(year_totals) >= 2:
            ordered_years = sorted(year_totals.keys())
            latest_year = ordered_years[-1]
            previous_year = ordered_years[-2]
            latest_total = year_totals[latest_year]
            previous_total = year_totals[previous_year]
            diff_absolute = latest_total - previous_total
            diff_pct = ((diff_absolute / previous_total) * 100) if previous_total else None
            if diff_pct is not None:
                diff_line = f"Variación {previous_year}->{latest_year}: {diff_absolute:,.0f} viajeros ({diff_pct:.2f}%)"
            else:
                diff_line = f"Variación {previous_year}->{latest_year}: {diff_absolute:,.0f} viajeros"
            comparison_lines.append(diff_line)

        summary_parts = []
        if top_entry:
            top_lines_text = "\n".join(top_lines)
            summary_parts.append(f"Top métricas:\n{top_lines_text}")

        if comparison_lines:
            summary_parts.append("Resumen por año:\n" + "\n".join(comparison_lines))

        if coverage_lines:
            summary_parts.append("Cobertura de datos:\n" + "\n".join(coverage_lines))

        quantitative_summary = ""
        if summary_parts:
            quantitative_summary = "Resúmenes cuantitativos:\n" + "\n\n".join(summary_parts) + "\n"

        formatting_prompt = f"""
Eres un asistente experto en análisis de datos de turismo de Andalucía.

Pregunta del usuario: "{question}"

{quantitative_summary}

Datos obtenidos de la base de datos:
{results_summary}

Total de registros: {len(results)}

Genera una respuesta clara y precisa que:
1. Responda directamente la pregunta
2. Incluya números específicos y métricas relevantes
3. Mencione comparaciones o variaciones interanuales si están disponibles
4. Liste las fuentes de datos al final entre corchetes
5. Use formato markdown para mejor legibilidad
6. Sea concisa pero completa (máximo 200 palabras)

IMPORTANTE:
- NO inventes números que no estén en los datos
- Si los datos son insuficientes, dilo claramente
- Incluye siempre las fuentes de archivos al final

Formato de ejemplo:
Según los datos de [periodo], hubo [número] de [métrica] en [categoría/provincia]...

📊 Datos clave:
- Métrica 1: [valor]
- Métrica 2: [valor]

📈 Variación interanual: [valor]%

[Fuentes: archivo1.xlsx, archivo2.xlsx]
"""

        try:
            response = self.model.generate_content(formatting_prompt)
            answer = response.text.strip()
            logger.info("Respuesta generada exitosamente")
            return answer

        except Exception as e:
            logger.error(f"Error al generar respuesta: {e}")
            return f"Error al formatear la respuesta: {str(e)}"

    def answer_question(self, question: str, save_history: bool = True) -> Dict[str, Any]:
        """
        Responde una pregunta completa: clasifica, extrae parámetros, consulta y formatea.

        Args:
            question: Pregunta del usuario
            save_history: Si True, guarda la interacción en el historial

        Returns:
            Dict con la respuesta y metadata
        """
        logger.info(f"Procesando pregunta: {question}")
        start_time = datetime.now()

        # Clasificar pregunta
        query_type = self.classify_question(question)

        if query_type == "rag":
            return {
                "question": question,
                "answer": "Esta pregunta requiere análisis de documentos PDF. Esta funcionalidad estará disponible próximamente.",
                "query_type": "rag",
                "error": "RAG not implemented yet"
            }

        # Extraer parámetros
        params = self.extract_query_parameters(question)

        # Construir query AQL
        aql_query, bind_vars = self.build_aql_query(params)

        # Ejecutar query
        results = self.execute_query(aql_query, bind_vars)

        # Formatear respuesta
        answer = self.format_answer(question, results, params)

        # Extraer fuentes únicas
        sources = list(set([r.get("fuente", "") for r in results if r.get("fuente")]))

        # Calcular tiempo de procesamiento
        duration = (datetime.now() - start_time).total_seconds()

        response_data = {
            "question": question,
            "answer": answer,
            "query_type": query_type,
            "parameters": params,
            "aql_query": aql_query,
            "bind_vars": bind_vars,
            "num_results": len(results),
            "sources": sources,
            "duration_seconds": duration,
            "timestamp": datetime.now().isoformat()
        }

        # Guardar en historial
        if save_history and self.db:
            self.db.save_qa_interaction(
                question=question,
                answer=answer,
                query_type=query_type,
                aql_query=aql_query,
                sources=sources
            )

        logger.info(f"Pregunta procesada en {duration:.2f}s")
        return response_data


if __name__ == "__main__":
    # Test del motor de consultas
    print("Inicializando motor de consultas Nexus...")

    engine = NexusQueryEngine()

    # Preguntas de ejemplo
    test_questions = [
        "¿Cuántos turistas británicos hubo en enero 2025?",
        "¿Cómo varió el turismo de cruceros entre 2024 y 2025?",
        "¿Qué provincia tuvo más turistas en 2024: Málaga o Sevilla?"
    ]

    test_questions = [
        # "¿Cuántos turistas franceses vinieron en abril de 2025 a Andalucía?",
        # "¿Cuántos turistas extranjeros de la Unión Europea han venido hasta lo que llevamos de datos de 2025?", #@TODO: falta datos por afuera. ie: britanicoas.
        # "¿Cuántas pernoctaciones en hoteles hubo en enero de 2025?",
        # "¿Cuál fue el país con mayor número de turistas extranjeros que visitaron Andalucía en marzo de 2025?",
        # "¿Cuál fue la media de gasto por turista internacional en Andalucía en el mes de abril de 2025?",

        "Compárame el número de turistas de la Unión Europea que han venido en el primer trimestre de 2025 con respecto al primer trimestre de 2024.",

        # "Si no contamos los turistas franceses ni ingleses, ¿cuántos turistas de la Unión Europea han venido en 2025?"
        # "¿Cuántas pernoctaciones de turistas franceses ha habido en el segundo trimestre de 2025 versus el de 2024?"
        # "¿Cuál ha sido la variación porcentual del gasto medio por turista internacional entre 2024 y 2025?"
        # "¿Qué porcentaje del total de turistas en Andalucía en el primer semestre de 2025 proviene de fuera de la Unión Europea?",

    ]

    print("\n" + "=" * 80)
    print("PRUEBAS DE CONSULTAS")
    print("=" * 80)

    for question in test_questions:
        print(f"\n❓ Pregunta: {question}")
        print("-" * 80)

        result = engine.answer_question(question, save_history=False)

        print(f"🤖 Respuesta:\n{result['answer']}")
        print(f"\n⏱️  Tiempo: {result.get('duration_seconds', -1):.2f}s")
        print(f"📊 Resultados: {result.get('num_results', '#N/A')}")
        print("=" * 80)
