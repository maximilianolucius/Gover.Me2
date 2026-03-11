#!/usr/bin/env python3
"""
CLI para el sistema Nexus de análisis de datos de turismo de Andalucía.
Proporciona comandos para ETL, consultas y gestión de datos.
"""

import argparse
import sys
import logging
from typing import Optional

if __package__:
    from .nexus_etl import run_etl
    from .nexus_query import NexusQueryEngine
    from .nexus_db import initialize_nexus_db
else:  # pragma: no cover - soporte para ejecución directa
    from nexus_etl import run_etl  # type: ignore
    from nexus_query import NexusQueryEngine  # type: ignore
    from nexus_db import initialize_nexus_db  # type: ignore

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cmd_etl(args):
    """Ejecuta el pipeline ETL."""
    print("\n" + "=" * 80)
    print("🔄 EJECUTANDO ETL PIPELINE")
    print("=" * 80)

    stats = run_etl(
        directory=args.directory,
        category_filter=args.category,
        year_filter=args.year,
        month_filter=args.month,
        clear_before=args.clear
    )

    if "error" in stats:
        print(f"\n❌ Error: {stats['error']}")
        return 1

    print(f"\n✅ ETL completado exitosamente!")
    print(f"\n📊 Resumen:")
    print(f"  • Archivos procesados: {stats['archivos_procesados']}")
    print(f"  • Métricas cargadas: {stats['metricas_cargadas']}")
    print(f"  • Duración: {stats['duracion_segundos']:.2f}s")

    if stats.get('estadisticas_db'):
        db_stats = stats['estadisticas_db']
        print(f"\n🗄️  Estado de la base de datos:")
        print(f"  • Total métricas en DB: {db_stats.get('total_metricas', 0)}")
        print(f"  • Años cubiertos: {', '.join(map(str, db_stats.get('anios_cubiertos', [])))}")
        print(f"  • Categorías: {len(db_stats.get('categorias', []))}")

    return 0


def cmd_query(args):
    """Ejecuta una consulta única."""
    print("\n" + "=" * 80)
    print("❓ CONSULTA NEXUS")
    print("=" * 80)

    if not args.question:
        print("❌ Error: Debes proporcionar una pregunta")
        return 1

    engine = NexusQueryEngine()
    result = engine.answer_question(args.question, save_history=not args.no_history)

    print(f"\n🤖 Respuesta:")
    print("-" * 80)
    print(result['answer'])
    print("-" * 80)

    if args.verbose:
        print(f"\n🔍 Detalles:")
        print(f"  • Tipo de query: {result['query_type']}")
        print(f"  • Resultados: {result['num_results']}")
        print(f"  • Duración: {result['duration_seconds']:.2f}s")
        if result.get('sources'):
            print(f"  • Fuentes: {', '.join(result['sources'][:5])}")

    return 0


def cmd_chat(args):
    """Modo chat interactivo."""
    print("\n" + "=" * 80)
    print("💬 MODO CHAT INTERACTIVO - NEXUS")
    print("=" * 80)
    print("\nEscribe tus preguntas sobre turismo en Andalucía.")
    print("Comandos especiales:")
    print("  • 'exit' o 'quit' - Salir")
    print("  • 'help' - Mostrar ayuda")
    print("  • 'stats' - Ver estadísticas de la DB")
    print("=" * 80 + "\n")

    engine = NexusQueryEngine()

    while True:
        try:
            # Leer pregunta del usuario
            question = input("👤 Tu pregunta: ").strip()

            # Comandos especiales
            if question.lower() in ['exit', 'quit', 'salir']:
                print("\n👋 ¡Hasta luego!")
                break

            if question.lower() == 'help':
                print("\n📚 Ejemplos de preguntas:")
                print("  • ¿Cuántos turistas británicos hubo en enero 2025?")
                print("  • ¿Cómo varió el turismo de cruceros entre 2024 y 2025?")
                print("  • ¿Qué provincia tuvo más turistas: Málaga o Sevilla?")
                print("  • ¿Cuál fue el gasto medio diario en el primer trimestre de 2024?")
                print("  • ¿Cuántas pernoctaciones hubo en Granada en verano 2024?\n")
                continue

            if question.lower() == 'stats':
                db = initialize_nexus_db()
                if db:
                    stats = db.get_stats()
                    print("\n📊 Estadísticas de la base de datos:")
                    print(f"  • Total métricas: {stats.get('total_metricas', 0)}")
                    print(f"  • Años: {', '.join(map(str, stats.get('anios_cubiertos', [])))}")
                    print(f"  • Categorías: {len(stats.get('categorias', []))}\n")
                    db.close()
                continue

            if not question:
                continue

            # Procesar pregunta
            print()
            result = engine.answer_question(question, save_history=True)

            print(f"🤖 {result['answer']}\n")

            if args.verbose:
                print(f"   ⏱️  {result['duration_seconds']:.2f}s | 📊 {result['num_results']} resultados\n")

        except KeyboardInterrupt:
            print("\n\n👋 ¡Hasta luego!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}\n")
            if args.verbose:
                import traceback
                traceback.print_exc()

    return 0


def cmd_stats(args):
    """Muestra estadísticas de la base de datos."""
    print("\n" + "=" * 80)
    print("📊 ESTADÍSTICAS DE LA BASE DE DATOS")
    print("=" * 80)

    db = initialize_nexus_db()
    if not db:
        print("\n❌ Error al conectar con la base de datos")
        return 1

    stats = db.get_stats()

    print(f"\n🗄️  Colección: metricas_turismo")
    print(f"  • Total de métricas: {stats.get('total_metricas', 0):,}")

    anios = stats.get('anios_cubiertos', [])
    if anios:
        print(f"\n📅 Años cubiertos: {', '.join(map(str, anios))}")
        print(f"  • Desde: {min(anios)}")
        print(f"  • Hasta: {max(anios)}")

    categorias = stats.get('categorias', [])
    if categorias:
        print(f"\n📁 Categorías ({len(categorias)}):")
        for cat_info in sorted(categorias, key=lambda x: x.get('count', 0), reverse=True)[:10]:
            categoria = cat_info.get('categoria', 'N/A')
            count = cat_info.get('count', 0)
            print(f"  • {categoria}: {count:,} métricas")

        if len(categorias) > 10:
            print(f"  ... y {len(categorias) - 10} más")

    db.close()
    print("\n" + "=" * 80)
    return 0


def cmd_clear(args):
    """Limpia la colección de métricas."""
    if not args.confirm:
        print("\n⚠️  ADVERTENCIA: Esta operación eliminará TODAS las métricas de la base de datos.")
        response = input("¿Estás seguro? Escribe 'CONFIRMAR' para continuar: ")
        if response != "CONFIRMAR":
            print("❌ Operación cancelada")
            return 1

    print("\n🗑️  Limpiando colección...")
    db = initialize_nexus_db()
    if not db:
        print("❌ Error al conectar con la base de datos")
        return 1

    if db.clear_collection("metricas_turismo"):
        print("✅ Colección limpiada exitosamente")
        return 0
    else:
        print("❌ Error al limpiar la colección")
        return 1


def main():
    """Punto de entrada principal del CLI."""
    parser = argparse.ArgumentParser(
        description="CLI para el sistema Nexus de análisis de datos de turismo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Ejecutar ETL completo
  python nexus_cli.py etl

  # ETL incremental para una categoría
  python nexus_cli.py etl --category cruceros --year 2025

  # Hacer una consulta
  python nexus_cli.py query "¿Cuántos turistas británicos hubo en enero 2025?"

  # Modo chat interactivo
  python nexus_cli.py chat

  # Ver estadísticas
  python nexus_cli.py stats
        """
    )

    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Mostrar información detallada')

    # Subcomandos
    subparsers = parser.add_subparsers(dest='command', help='Comando a ejecutar')

    # Comando ETL
    parser_etl = subparsers.add_parser('etl', help='Ejecutar pipeline ETL')
    parser_etl.add_argument('-d', '--directory', default='nexus',
                           help='Directorio con archivos Excel (default: nexus)')
    parser_etl.add_argument('-c', '--category',
                           help='Filtrar por categoría específica')
    parser_etl.add_argument('-y', '--year', type=int,
                           help='Filtrar por año específico')
    parser_etl.add_argument('-m', '--month', type=int,
                           help='Filtrar por mes específico (1-12)')
    parser_etl.add_argument('--clear', action='store_true',
                           help='Limpiar colección antes de cargar')
    parser_etl.set_defaults(func=cmd_etl)

    # Comando query
    parser_query = subparsers.add_parser('query', help='Hacer una consulta')
    parser_query.add_argument('question', nargs='*',
                             help='Pregunta sobre datos de turismo')
    parser_query.add_argument('--no-history', action='store_true',
                             help='No guardar en el historial')
    parser_query.set_defaults(func=cmd_query)

    # Comando chat
    parser_chat = subparsers.add_parser('chat', help='Modo chat interactivo')
    parser_chat.set_defaults(func=cmd_chat)

    # Comando stats
    parser_stats = subparsers.add_parser('stats', help='Ver estadísticas de la DB')
    parser_stats.set_defaults(func=cmd_stats)

    # Comando clear
    parser_clear = subparsers.add_parser('clear', help='Limpiar colección de métricas')
    parser_clear.add_argument('--confirm', action='store_true',
                             help='Confirmar sin preguntar')
    parser_clear.set_defaults(func=cmd_clear)

    # Parsear argumentos
    args = parser.parse_args()

    # Si es comando query, unir todos los argumentos en una pregunta
    if hasattr(args, 'question') and isinstance(args.question, list):
        args.question = ' '.join(args.question)

    # Ejecutar comando
    if hasattr(args, 'func'):
        try:
            return args.func(args)
        except KeyboardInterrupt:
            print("\n\n⚠️  Operación interrumpida por el usuario")
            return 130
        except Exception as e:
            logger.error(f"Error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
