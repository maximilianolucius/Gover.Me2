---
  # Sistema de Análisis de Datos de Turismo para Andalucía

  ## Contexto

  Tengo datos estadísticos de turismo de Andalucía en formato Excel (.xlsx) y PDF.
  Necesito construir un sistema de Q&A que permita hacer preguntas en lenguaje
  natural sobre estos datos y obtener respuestas precisas basadas en los números
  reales.

  ## Estructura de los Datos

  ### Archivos Excel (Ubicación: `nexus/`)

  **Patrón de nombres:**
  - Formato: `{numero}_{categoria}_{mes}{año}.xlsx`
  - Ejemplos:
    - `01_total_turistas_ene25.xlsx`
    - `11_cruceros_feb24.xlsx`
    - `06_britanicos_mar25_limpio.xlsx`

  **21 Categorías diferentes:**
  1. Total turistas
  2. Españoles
  3. Andaluces
  4. Resto España
  5. Extranjeros
  6. Británicos
  7. Alemanes
  8. Otros mercados
  9. Litoral
  10. Interior
  11. Cruceros
  12. Ciudad
  13. Cultural
  14. Almería
  15. Cádiz
  16. Córdoba
  17. Granada
  18. Huelva
  19. Jaén
  20. Málaga
  21. Sevilla

  **Periodos cubiertos:**
  - Datos mensuales desde enero 2023 hasta junio 2025
  - Algunos archivos tienen sufijo "_limpio"

  **Contenido de los Excels:**
  - Sheet principal: "ficha"
  - Métricas incluidas:
    - Número de viajeros en establecimientos hoteleros
    - Número de pernoctaciones
    - Cuota (% sobre total España)
    - Estancia media (días)
    - Gasto medio diario (euros)
    - Número de turistas (millones)
    - Variación/diferencia interanual
    - Llegadas de pasajeros a aeropuertos
  - Datos organizados en: mensuales, acumulado año, año completo

  ### Archivos PDF (Ubicación: `nexus/`)

  **Patrón de nombres:**
  - Formato: `ultimos-datos_{mes}{año}.pdf`
  - Ejemplos: `ultimos-datos_ene23.pdf`, `ultimos-datos_jun24.pdf`
  - Contienen: Reportes visuales, gráficos, análisis narrativo

  ## Objetivos del Sistema

  Crear un sistema capaz de responder preguntas como:

  1. **Queries numéricas simples:**
     - "¿Cuántos turistas británicos hubo en enero 2025?"
     - "¿Cuál fue el gasto medio diario en marzo 2024?"

  2. **Comparaciones temporales:**
     - "¿Hubo más turismo en 2023 respecto a 2024?"
     - "¿Cómo varió el turismo de cruceros entre Q1 2024 y Q1 2025?"

  3. **Agregaciones:**
     - "¿Cuántos turistas ingresaron por crucero en el primer trimestre de 2024?"
     - "¿Cuál fue el total de pernoctaciones en Málaga durante el verano 2024?"

  4. **Comparaciones entre categorías:**
     - "¿Qué provincia tuvo más turistas en 2024: Málaga o Sevilla?"
     - "¿Hubo más turistas británicos o alemanes en diciembre 2023?"

  5. **Análisis de tendencias:**
     - "¿Cuál es la tendencia de turismo en el litoral vs interior en 2024?"
     - "¿En qué meses del 2024 hubo mayor variación interanual?"

  ## Requisitos Técnicos

  ### Stack Tecnológico
  - **Lenguaje:** Python 3.10+
  - **Base de datos:** ArangoDB
  - **LLM:** Gemini (APIs disponibles)
  - **CLI:** Python argparse

  ### Arquitectura Propuesta

  **Opción preferida: Hybrid (ETL + Text-to-SQL + RAG)**

  Excel Files → ETL → ArangoDB → Text-to-SQL → Results → LLM Format → Answer
                                       ↓
  PDF Files  → RAG Pipeline → Embeddings → FAISS → Context → LLM → Answer

  **Componentes a implementar:**

  1. **ETL Pipeline (`nexus_etl.py`):**
     - Función para parsear nombres de archivos y extraer metadata
       (categoría, mes, año)
     - Función para leer Excel files usando `openpyxl` o `pandas`
     - Función para extraer métricas de la sheet "ficha"
     - Función para cargar datos a ArangoDB en una nueva colección
       `metricas_turismo`
     - Función para procesar todos los archivos del directorio `nexus/`
     - Logging detallado del proceso
     - Manejo de errores (archivos corruptos, formatos inconsistentes)

  2. **Schema de Base de Datos:**
     - Colección: `metricas_turismo` en ArangoDB
     - Campos sugeridos:
       ```json
       {
         "categoria": "cruceros",
         "mes": 1,
         "año": 2025,
         "periodo_tipo": "mensual|acumulado|anual",
         "metrica_nombre": "viajeros",
         "metrica_valor": 45000,
         "variacion_interanual": -5.4,
         "provincia": "Málaga" (opcional),
         "fuente_archivo": "11_cruceros_ene25.xlsx",
         "timestamp_ingestion": "2025-01-15T10:30:00"
       }
       ```
     - Índices: categoria, año, mes, metrica_nombre

  3. **Text-to-SQL Generator (`nexus_query.py`):**
     - Función que usa LLM para convertir pregunta natural a parámetros
       estructurados
     - Función que genera AQL (ArangoDB Query Language) basada en parámetros
     - Validador de queries (prevenir queries peligrosas)
     - Ejecutor de queries con error handling
     - Función que formatea resultados numéricos a respuesta natural con LLM

  4. **RAG para PDFs:**
     - Reutilizar pipeline existente del proyecto para procesar PDFs
     - Integrar con el sistema de queries para información cualitativa

  5. **Clasificador de Preguntas:**
     - Función que determina si una pregunta requiere:
       - SQL query (datos numéricos/estructurados)
       - RAG sobre PDFs (información cualitativa/gráficos)
       - Ambos (respuesta híbrida)

  6. **Interfaz Gradio (`nexus_app.py`):**
     - UI similar a `app.py` existente pero para datos de turismo
     - Selector de periodo (mes/año)
     - Selector de categoría (opcional para filtrar)
     - Chatbot para hacer preguntas
     - Visualización opcional de resultados (tablas, gráficos)
     - Exportar resultados a CSV/Excel

  7. **CLI (`nexus_cli.py`):**
     - Comandos:
       - `python nexus_cli.py etl` - Ejecutar ETL completo
       - `python nexus_cli.py etl --category cruceros --year 2025` - ETL parcial
       - `python nexus_cli.py query "¿Cuántos turistas...?"` - Query única
       - `python nexus_cli.py chat` - Modo interactivo
       - `python nexus_cli.py stats` - Mostrar estadísticas de la DB

  8. **Almacenamiento de Q&A:**
     - Reutilizar colección `qa_history` existente
     - Agregar campos específicos para queries SQL generadas

  9. **Testing:**
     - Script de prueba con preguntas de ejemplo
     - Validación de respuestas contra valores conocidos

  ## Restricciones y Consideraciones

  1. **No modificar código existente** del sistema PDF Q&A actual
  2. Crear módulos separados en directorio `nexus/` o similar
  3. Reutilizar infraestructura existente donde sea posible:
     - ArangoDB client
     - Gemini/Mistral helpers
     - Storage backend
     - Q&A history
  4. Manejar archivos con sufijo "_limpio" vs sin sufijo
     (pueden tener diferentes estructuras)
  5. Los meses están en español abreviado: ene, feb, mar, abr, may, jun,
     jul, ago, sep, oct, nov, dic
  6. Algunos archivos pueden tener datos faltantes o estructuras inconsistentes
  7. Priorizar precisión numérica sobre velocidad de respuesta
  8. Incluir siempre fuente de datos en las respuestas
     (ej: "Según datos de enero 2025...")

  ## Dependencias Adicionales Necesarias

  ```txt
  openpyxl>=3.1.0  # Para leer Excel files
  python-dateutil>=2.8.0  # Para parseo de fechas
  plotly>=5.0.0  # Para visualizaciones (opcional)

  Ejemplos de Uso Esperado

  CLI:

  # Ejecutar ETL inicial
  python nexus_cli.py etl
  # Output: "Procesados 126 archivos Excel, 2,834 métricas cargadas en DB"

  # Hacer una pregunta
  python nexus_cli.py query "¿Cuántos turistas británicos hubo en enero 2025?"
  # Output:
  # "Según los datos de enero 2025, hubo aproximadamente 156,000 turistas
  #  británicos en Andalucía, lo que representa una disminución del 3.2%
  #  respecto a enero 2024.
  #  [Fuente: 06_britanicos_ene25.xlsx]"

  # Modo chat interactivo
  python nexus_cli.py chat

  Gradio UI:

  Usuario: "Compara el turismo de cruceros entre Q1 2024 y Q1 2025"

  Sistema:
  📊 Comparación Turismo de Cruceros Q1

  Primer Trimestre 2024: 245,000 turistas
  Primer Trimestre 2025: 268,000 turistas

  📈 Variación: +9.4% (23,000 turistas más)

  Desglose mensual:
  - Enero: +5.2%
  - Febrero: +12.1%
  - Marzo: +10.8%

  [Fuentes: 11_cruceros_ene24.xlsx, 11_cruceros_feb24.xlsx,
            11_cruceros_mar24.xlsx, 11_cruceros_ene25.xlsx,
            11_cruceros_feb25.xlsx, 11_cruceros_mar25.xlsx]

  Deliverables

  Por favor, crea:

  1. ✅ ETL script completo y funcional
  2. ✅ Módulo de query con Text-to-SQL
  3. ✅ Extensión de ArangoDB schema con nueva colección
  4. ✅ Gradio web app para Q&A sobre datos de turismo
  5. ✅ CLI con comandos útiles
  6. ✅ Script de testing con casos de prueba
  7. ✅ README con instrucciones de uso
  8. ✅ Ejemplos de queries soportadas
  9. ✅ (Opcional) Notebook Jupyter con análisis exploratorio de los datos

  Estilo de Código

  - Usar type hints
  - Docstrings en español
  - Logging apropiado
  - Error handling robusto
  - Tests unitarios cuando sea posible

  Notas Importantes

  - El sistema debe ser escalable: cuando lleguen datos de julio 2025,
  solo ejecutar ETL incremental
  - Las respuestas deben ser auditables: siempre incluir fuente de datos
  - Priorizar precisión sobre generación de lenguaje natural
  - Si el LLM no tiene suficiente información, debe decir "No tengo datos
  suficientes para responder" en lugar de inventar números

  ---
  Comienza con el ETL, luego el sistema de queries, y finalmente las interfaces.
  Pregunta si necesitas aclaraciones sobre la estructura de los datos o los
  requisitos.

  ---

  ## 💡 **Explicación del Prompt**

  Este prompt es efectivo porque:

  1. **Contexto claro**: Explica qué problema resolver
  2. **Datos bien definidos**: Estructura exacta de archivos y contenido
  3. **Objetivos específicos**: Ejemplos concretos de preguntas a responder
  4. **Arquitectura sugerida**: Propone solución pero deja espacio para mejoras
  5. **Restricciones explícitas**: No tocar código existente, reutilizar infraestructura
  6. **Ejemplos de uso**: Muestra comportamiento esperado
  7. **Deliverables concretos**: Lista exacta de lo que debe generar
  8. **Estilo de código**: Mantener consistencia con el proyecto
