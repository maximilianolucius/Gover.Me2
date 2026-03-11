#!/usr/bin/env python3
"""
Script para probar las 4 consultas problemáticas
"""

if __package__:
    from .nexus_query import NexusQueryEngine
else:  # pragma: no cover - ejecución directa
    from nexus_query import NexusQueryEngine  # type: ignore

# Inicializar engine
engine = NexusQueryEngine()

# Las 4 consultas que el usuario reportó
queries = [
    "¿Cuántos turistas hubo en agosto 2023?",
    "¿Cómo fue el turismo en 2022 vs 2023?",
    "¿Qué mes de 2024 tuvo más turistas?",
    "¿Cuántos turistas de cruceros hubo en diciembre 2022?"
]

print("=" * 80)
print("PROBANDO LAS 4 CONSULTAS")
print("=" * 80)
print()

for i, query in enumerate(queries, 1):
    print(f"{i}. {query}")

    try:
        # Clasificar
        query_type = engine.classify_question(query)
        print(f"   Tipo: {query_type}")

        # Extraer parámetros
        params = engine.extract_query_parameters(query)
        print(f"   Categorías: {params.get('categorias', [])}")
        print(f"   Años: {params.get('anios', [])}")
        print(f"   Meses: {params.get('meses', [])}")

        # Ejecutar query
        aql, bind_vars = engine.build_aql_query(params)
        results = engine.execute_query(aql, bind_vars)

        print(f"   ✅ Resultados: {len(results)} registros encontrados")

        if results:
            # Mostrar primer resultado
            first = results[0]
            print(f"   📊 Ejemplo: {first.get('metrica_nombre')}: {first.get('metrica_valor')}")
        else:
            print(f"   ⚠️  No se encontraron datos")

    except Exception as e:
        print(f"   ❌ Error: {e}")

    print()

print("=" * 80)
print("RESUMEN:")
print("Si todas las consultas muestran '✅ Resultados: N registros', entonces el")
print("problema de encoding y mapeo semántico ha sido resuelto.")
print("=" * 80)
