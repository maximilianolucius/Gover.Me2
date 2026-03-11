"""
Script de testing para el sistema Nexus de análisis de datos de turismo.
Valida componentes principales y respuestas del sistema.
"""

import sys
import time
from typing import Dict, List, Any
from datetime import datetime

if __package__:
    from .nexus_db import initialize_nexus_db
    from .nexus_query import NexusQueryEngine
    from .nexus_etl import parse_filename
else:  # pragma: no cover - ejecución directa
    from nexus_db import initialize_nexus_db  # type: ignore
    from nexus_query import NexusQueryEngine  # type: ignore
    from nexus_etl import parse_filename  # type: ignore


class TestResult:
    """Clase para almacenar resultados de tests."""

    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.message = ""
        self.duration = 0.0

    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        return f"{status} - {self.name} ({self.duration:.2f}s)\n    {self.message}"


class NexusTestSuite:
    """Suite de pruebas para el sistema Nexus."""

    def __init__(self):
        self.results: List[TestResult] = []
        self.db = None
        self.query_engine = None

    def run_all_tests(self):
        """Ejecuta todos los tests."""
        print("\n" + "=" * 80)
        print("🧪 NEXUS TEST SUITE")
        print("=" * 80 + "\n")

        # Tests de conexión
        self.test_db_connection()

        # Tests de ETL
        self.test_filename_parsing()

        # Tests de consultas
        if self.query_engine:
            self.test_question_classification()
            self.test_parameter_extraction()
            self.test_query_execution()

        # Tests de integración
        if self.query_engine:
            self.test_end_to_end_queries()

        # Mostrar resumen
        self.print_summary()

    def test_db_connection(self):
        """Test de conexión a la base de datos."""
        result = TestResult("Conexión a ArangoDB")
        start = time.time()

        try:
            self.db = initialize_nexus_db()
            if self.db:
                stats = self.db.get_stats()
                total = stats.get('total_metricas', 0)
                result.passed = True
                result.message = f"Conectado exitosamente. {total:,} métricas en DB"

                # Inicializar query engine si la conexión es exitosa
                self.query_engine = NexusQueryEngine(db=self.db)
            else:
                result.message = "No se pudo inicializar la base de datos"

        except Exception as e:
            result.message = f"Error: {str(e)}"

        result.duration = time.time() - start
        self.results.append(result)

    def test_filename_parsing(self):
        """Test de parseo de nombres de archivo."""
        result = TestResult("Parseo de nombres de archivo")
        start = time.time()

        test_cases = [
            ("01_total_turistas_ene25.xlsx", {
                "categoria": "total_turistas",
                "mes": 1,
                "anio": 2025,
                "es_limpio": False
            }),
            ("11_cruceros_feb24_limpio.xlsx", {
                "categoria": "cruceros",
                "mes": 2,
                "anio": 2024,
                "es_limpio": True
            }),
            ("20_malaga_mar25.xlsx", {
                "categoria": "malaga",
                "mes": 3,
                "anio": 2025,
                "es_limpio": False
            })
        ]

        passed = 0
        failed = 0

        for filename, expected in test_cases:
            metadata = parse_filename(filename)
            if metadata:
                if (metadata["categoria"] == expected["categoria"] and
                    metadata["mes"] == expected["mes"] and
                    metadata["anio"] == expected["anio"] and
                    metadata["es_limpio"] == expected["es_limpio"]):
                    passed += 1
                else:
                    failed += 1
            else:
                failed += 1

        result.passed = (failed == 0)
        result.message = f"{passed}/{len(test_cases)} casos pasaron"

        result.duration = time.time() - start
        self.results.append(result)

    def test_question_classification(self):
        """Test de clasificación de preguntas."""
        result = TestResult("Clasificación de preguntas")
        start = time.time()

        test_cases = [
            ("¿Cuántos turistas británicos hubo en enero 2025?", "sql"),
            ("¿Cuál fue el gasto medio diario en marzo 2024?", "sql"),
        ]

        try:
            passed = 0
            for question, expected_type in test_cases:
                classification = self.query_engine.classify_question(question)
                if classification == expected_type:
                    passed += 1

            result.passed = (passed == len(test_cases))
            result.message = f"{passed}/{len(test_cases)} clasificaciones correctas"

        except Exception as e:
            result.message = f"Error: {str(e)}"

        result.duration = time.time() - start
        self.results.append(result)

    def test_parameter_extraction(self):
        """Test de extracción de parámetros."""
        result = TestResult("Extracción de parámetros")
        start = time.time()

        test_questions = [
            "¿Cuántos turistas británicos hubo en enero 2025?",
            "¿Cómo varió el turismo de cruceros entre 2024 y 2025?"
        ]

        try:
            passed = 0
            for question in test_questions:
                params = self.query_engine.extract_query_parameters(question)
                if isinstance(params, dict) and "categorias" in params:
                    passed += 1

            result.passed = (passed == len(test_questions))
            result.message = f"{passed}/{len(test_questions)} extracciones exitosas"

        except Exception as e:
            result.message = f"Error: {str(e)}"

        result.duration = time.time() - start
        self.results.append(result)

    def test_query_execution(self):
        """Test de ejecución de queries AQL."""
        result = TestResult("Ejecución de queries AQL")
        start = time.time()

        try:
            # Query simple para obtener métricas recientes
            aql = """
            FOR doc IN metricas_turismo
                FILTER doc.anio >= 2024
                LIMIT 10
                RETURN doc
            """

            results = self.db.query_metrics(aql)

            result.passed = len(results) > 0
            result.message = f"Query ejecutada: {len(results)} resultados"

        except Exception as e:
            result.message = f"Error: {str(e)}"

        result.duration = time.time() - start
        self.results.append(result)

    def test_end_to_end_queries(self):
        """Test end-to-end con preguntas reales."""
        result = TestResult("Consultas end-to-end")
        start = time.time()

        test_questions = [
            "¿Cuántos turistas hubo en enero 2025?",
            "¿Cuál fue el turismo en Málaga en 2024?",
        ]

        try:
            passed = 0
            for question in test_questions:
                response = self.query_engine.answer_question(question, save_history=False)

                if (response and
                    "answer" in response and
                    response["answer"] and
                    "No tengo datos" not in response["answer"]):
                    passed += 1

            result.passed = (passed > 0)  # Al menos una debe pasar
            result.message = f"{passed}/{len(test_questions)} consultas exitosas"

        except Exception as e:
            result.message = f"Error: {str(e)}"

        result.duration = time.time() - start
        self.results.append(result)

    def print_summary(self):
        """Imprime resumen de resultados."""
        print("\n" + "=" * 80)
        print("📊 RESULTADOS")
        print("=" * 80 + "\n")

        for result in self.results:
            print(result)
            print()

        # Estadísticas
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed

        print("=" * 80)
        print(f"Total: {total} | ✅ Pasados: {passed} | ❌ Fallidos: {failed}")
        print("=" * 80 + "\n")

        return failed == 0


def run_quick_validation():
    """
    Ejecuta una validación rápida del sistema con preguntas de ejemplo.
    """
    print("\n" + "=" * 80)
    print("🔍 VALIDACIÓN RÁPIDA DEL SISTEMA")
    print("=" * 80 + "\n")

    # Inicializar motor de consultas
    engine = NexusQueryEngine()

    # Preguntas de ejemplo
    questions = [
        "¿Cuántos turistas británicos hubo en enero 2025?",
        "¿Cuál fue el gasto medio diario en 2024?",
        "¿Qué provincia tuvo más turistas: Málaga o Sevilla?",
    ]

    print("Ejecutando preguntas de validación...\n")

    for i, question in enumerate(questions, 1):
        print(f"\n{'=' * 80}")
        print(f"📝 Pregunta {i}: {question}")
        print('=' * 80)

        try:
            result = engine.answer_question(question, save_history=False)

            print(f"\n🤖 Respuesta:")
            print(f"{result['answer']}")

            print(f"\n📊 Metadata:")
            print(f"  • Tipo: {result['query_type']}")
            print(f"  • Resultados: {result['num_results']}")
            print(f"  • Duración: {result['duration_seconds']:.2f}s")

            if result.get('sources'):
                print(f"  • Fuentes: {len(result['sources'])} archivos")

        except Exception as e:
            print(f"\n❌ Error: {str(e)}")

    print("\n" + "=" * 80)
    print("✅ Validación completada")
    print("=" * 80 + "\n")


def main():
    """Punto de entrada principal."""
    import argparse

    parser = argparse.ArgumentParser(description="Test suite para Nexus")
    parser.add_argument("--quick", action="store_true",
                       help="Ejecutar validación rápida en lugar de tests completos")

    args = parser.parse_args()

    if args.quick:
        run_quick_validation()
    else:
        suite = NexusTestSuite()
        suite.run_all_tests()

        # Retornar código de salida apropiado
        all_passed = all(r.passed for r in suite.results)
        sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
