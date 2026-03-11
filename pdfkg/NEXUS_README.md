# 🏖️ Nexus - Sistema de Análisis de Datos de Turismo en Andalucía

Sistema avanzado de Q&A que permite hacer preguntas en lenguaje natural sobre datos estadísticos de turismo de Andalucía y obtener respuestas precisas basadas en métricas reales.

## 📋 Tabla de Contenidos

- [Características](#características)
- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
  - [CLI](#cli)
  - [Interfaz Web](#interfaz-web)
  - [Como Módulo Python](#como-módulo-python)
- [Datos](#datos)
- [Ejemplos de Consultas](#ejemplos-de-consultas)
- [Testing](#testing)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Troubleshooting](#troubleshooting)

## ✨ Características

- **🤖 Consultas en Lenguaje Natural:** Haz preguntas como hablas, sin necesidad de conocer SQL
- **📊 Análisis de Datos Reales:** Basado en datos oficiales de turismo de Andalucía (2023-2025)
- **🔄 ETL Automatizado:** Pipeline para procesar y cargar archivos Excel automáticamente
- **💬 Múltiples Interfaces:** CLI, Web UI (Gradio), y API programática
- **📈 Visualizaciones:** Gráficos interactivos de distribución de datos
- **🎯 Text-to-SQL:** Convierte preguntas naturales a queries AQL optimizadas
- **📁 21 Categorías:** Total turistas, británicos, alemanes, cruceros, provincias, etc.
- **🗄️ ArangoDB:** Base de datos NoSQL de alto rendimiento
- **🧠 Gemini AI:** Powered by Google's Gemini para comprensión del lenguaje

## 🏗️ Arquitectura

```
┌─────────────────┐
│  Excel Files    │
│  (nexus/*.xlsx) │
└────────┬────────┘
         │
         ▼
  ┌─────────────┐
  │ ETL Pipeline│
  │ (nexus_etl) │
  └──────┬──────┘
         │
         ▼
  ┌─────────────────┐
  │   ArangoDB      │
  │ metricas_turismo│
  └────────┬────────┘
           │
           ▼
    ┌──────────────────┐
    │  Query Engine    │
    │ (nexus_query)    │
    │  • Classification │
    │  • Text-to-SQL   │
    │  • Gemini AI     │
    └─────┬────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌──────────┐
│  CLI   │  │ Gradio UI│
└────────┘  └──────────┘
```

### Flujo de Datos

1. **ETL:** Archivos Excel → Parsing → Transformación → ArangoDB
2. **Query:** Pregunta → Clasificación → Extracción de parámetros → AQL → Resultados
3. **Formato:** Resultados → Gemini AI → Respuesta en lenguaje natural

## 📦 Requisitos

- **Python:** 3.10 o superior
- **ArangoDB:** 3.10+ (debe estar ejecutándose)
- **Gemini API Key:** Para el procesamiento de lenguaje natural
- **Memoria:** Mínimo 4GB RAM
- **Espacio en disco:** ~1GB para datos y dependencias

## 🚀 Instalación

### 1. Clonar o preparar el proyecto

```bash
cd /home/maxim/PycharmProjects/Gover.Me2/pdfkg
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

Edita el archivo `.env`:

```bash
GEMINI_API_KEY=tu_api_key_aqui
GEMINI_MODEL=gemini-2.0-flash-exp

STORAGE_BACKEND=arango

ARANGO_HOST=localhost
ARANGO_PORT=8529
ARANGO_USER=root
ARANGO_PASSWORD=
ARANGO_DB=pdfkg
```

### 4. Verificar ArangoDB

Asegúrate de que ArangoDB esté ejecutándose:

```bash
# Verificar que ArangoDB está activo
curl http://localhost:8529
```

### 5. Ejecutar ETL inicial

```bash
python nexus_cli.py etl
```

## ⚙️ Configuración

### Estructura de Archivos Excel

Los archivos deben seguir este patrón de nombres:

```
{numero}_{categoria}_{mes}{año}.xlsx
{numero}_{categoria}_{mes}{año}_limpio.xlsx

Ejemplos:
  01_total_turistas_ene25.xlsx
  11_cruceros_feb24.xlsx
  20_malaga_mar25_limpio.xlsx
```

### Categorías Soportadas

| Código | Categoría | Código | Categoría |
|--------|-----------|--------|-----------|
| 01 | total_turistas | 11 | cruceros |
| 02 | espanoles | 12 | ciudad |
| 03 | andaluces | 13 | cultural |
| 04 | resto_espana | 14-21 | provincias* |
| 05 | extranjeros | | |
| 06 | britanicos | | |
| 07 | alemanes | | |
| 08 | otros_mercados | | |
| 09 | litoral | | |
| 10 | interior | | |

*Provincias: Almería, Cádiz, Córdoba, Granada, Huelva, Jaén, Málaga, Sevilla

## 📖 Uso

### CLI

#### Ejecutar ETL completo

```bash
python nexus_cli.py etl
```

#### ETL incremental (filtrado)

```bash
# Solo una categoría
python nexus_cli.py etl --category cruceros

# Solo un año
python nexus_cli.py etl --year 2025

# Combinado
python nexus_cli.py etl --category malaga --year 2024 --month 6
```

#### Limpiar y recargar datos

```bash
python nexus_cli.py etl --clear
```

#### Hacer una consulta

```bash
python nexus_cli.py query "¿Cuántos turistas británicos hubo en enero 2025?"
```

#### Modo chat interactivo

```bash
python nexus_cli.py chat
```

Dentro del chat:
- Escribe preguntas naturalmente
- `help` - Ver ejemplos
- `stats` - Ver estadísticas de la DB
- `exit` - Salir

#### Ver estadísticas

```bash
python nexus_cli.py stats
```

### Interfaz Web

#### Lanzar la aplicación web

```bash
python nexus_app.py
```

La interfaz estará disponible en: `http://localhost:7860`

#### Opciones de lanzamiento

```bash
# Puerto personalizado
python nexus_app.py --port 8080

# Crear enlace público (Gradio share)
python nexus_app.py --share
```

### Como Módulo Python

```python
from nexus_query import NexusQueryEngine
from nexus_db import initialize_nexus_db

# Inicializar
engine = NexusQueryEngine()

# Hacer pregunta
result = engine.answer_question(
    "¿Cuántos turistas británicos hubo en enero 2025?",
    save_history=True
)

print(result['answer'])
print(f"Resultados: {result['num_results']}")
print(f"Duración: {result['duration_seconds']:.2f}s")
```

## 💾 Datos

### Métricas Disponibles

- **Número de viajeros** en establecimientos hoteleros
- **Número de pernoctaciones** en establecimientos hoteleros
- **Cuota** (% sobre total pernoctaciones en España)
- **Llegadas de pasajeros** a aeropuertos andaluces
- **Número de turistas** (millones)
- **Estancia media** (número de días)
- **Gasto medio diario** (euros)
- **Variación interanual** (%)

### Periodos

- **Mensual:** Datos de un mes específico
- **Acumulado:** Datos acumulados del año hasta ese mes
- **Anual:** Datos del año completo

### Cobertura

- **Periodo:** Enero 2023 - Mayo 2025
- **Frecuencia:** Mensual
- **Fuente:** Oficina del Dato - Turismo y Deporte de Andalucía

## 🎯 Ejemplos de Consultas

### Queries Numéricas Simples

```
¿Cuántos turistas británicos hubo en enero 2025?
¿Cuál fue el gasto medio diario en marzo 2024?
¿Cuántas pernoctaciones hubo en Granada en febrero 2025?
```

### Comparaciones Temporales

```
¿Hubo más turismo en 2023 respecto a 2024?
¿Cómo varió el turismo de cruceros entre 2024 y 2025?
¿Cuál fue la variación interanual de turistas británicos en enero 2025?
```

### Agregaciones

```
¿Cuántos turistas ingresaron por crucero en el primer trimestre de 2024?
¿Cuál fue el total de pernoctaciones en Málaga durante el verano 2024?
¿Cuántos turistas extranjeros hubo en el año 2024?
```

### Comparaciones entre Categorías

```
¿Qué provincia tuvo más turistas en 2024: Málaga o Sevilla?
¿Hubo más turistas británicos o alemanes en diciembre 2023?
¿Dónde hubo más turismo: litoral o interior en 2024?
```

### Análisis de Tendencias

```
¿Cuál es la tendencia de turismo en el litoral vs interior en 2024?
¿En qué meses del 2024 hubo mayor variación interanual?
¿Cómo fue el turismo cultural a lo largo de 2024?
```

## 🧪 Testing

### Ejecutar suite completa de tests

```bash
python test_nexus.py
```

Tests incluidos:
- ✅ Conexión a ArangoDB
- ✅ Parseo de nombres de archivo
- ✅ Clasificación de preguntas
- ✅ Extracción de parámetros
- ✅ Ejecución de queries AQL
- ✅ Consultas end-to-end

### Validación rápida

```bash
python test_nexus.py --quick
```

Ejecuta preguntas de ejemplo para validar que el sistema funciona.

## 📁 Estructura del Proyecto

```
pdfkg/
├── nexus/                      # Archivos de datos
│   ├── *.xlsx                  # Archivos Excel de turismo
│   └── *.pdf                   # Reportes PDF (futura integración)
├── nexus_db.py                 # Configuración y gestión de ArangoDB
├── nexus_etl.py                # Pipeline ETL para procesar Excel
├── nexus_query.py              # Motor de consultas Text-to-SQL
├── nexus_cli.py                # Interfaz de línea de comandos
├── nexus_app.py                # Interfaz web Gradio
├── test_nexus.py               # Suite de pruebas
├── requirements.txt            # Dependencias Python
├── .env                        # Variables de entorno
└── NEXUS_README.md            # Este archivo
```

## 🔧 Troubleshooting

### Error: "No se pudo conectar a ArangoDB"

**Solución:**
```bash
# Verificar que ArangoDB está ejecutándose
systemctl status arangodb3

# O iniciar manualmente
arangod
```

### Error: "Gemini API Key not configured"

**Solución:**
Verifica que tu `.env` tiene la clave API:
```bash
GEMINI_API_KEY=tu_api_key_valida
```

### ETL no encuentra archivos

**Solución:**
Verifica que los archivos están en el directorio correcto:
```bash
ls nexus/*.xlsx | head
```

### Query devuelve "No tengo datos suficientes"

**Posibles causas:**
1. ETL no se ha ejecutado: `python nexus_cli.py etl`
2. Base de datos vacía: Verificar con `python nexus_cli.py stats`
3. Pregunta fuera del rango de datos disponibles

### Errores de memoria con muchos archivos

**Solución:**
Procesar en lotes:
```bash
python nexus_cli.py etl --year 2025
python nexus_cli.py etl --year 2024
python nexus_cli.py etl --year 2023
```

## 📊 Esquema de Base de Datos

### Colección: `metricas_turismo`

```json
{
  "categoria": "cruceros",
  "mes": 1,
  "mes_str": "ene",
  "anio": 2025,
  "periodo_tipo": "mensual",
  "metrica_nombre": "Número de turistas (millones)",
  "metrica_valor": 0.245,
  "variacion_interanual": -0.032,
  "periodo_descripcion": "enero - 2025",
  "provincia": "Málaga",  // opcional
  "fuente_archivo": "11_cruceros_ene25.xlsx",
  "es_limpio": false,
  "timestamp_ingestion": "2025-01-15T10:30:00"
}
```

### Índices

- `categoria`
- `anio`
- `mes`
- `metrica_nombre`
- `periodo_tipo`
- Compuesto: `[categoria, anio, mes]`

## 🤝 Contribuir

Para contribuir mejoras:

1. Prueba tus cambios con `python test_nexus.py`
2. Asegúrate de que el ETL funciona correctamente
3. Valida que las consultas devuelven respuestas precisas
4. Actualiza la documentación si es necesario

## 📝 Notas Técnicas

### Text-to-SQL Pipeline

1. **Clasificación:** Determina si la pregunta requiere SQL, RAG o híbrido
2. **Extracción:** Usa Gemini para extraer parámetros (categorías, fechas, métricas)
3. **Generación:** Construye query AQL basada en parámetros
4. **Ejecución:** Ejecuta query en ArangoDB
5. **Formato:** Gemini convierte resultados a lenguaje natural

### Optimizaciones

- Índices en campos frecuentemente consultados
- Límite de 100 resultados por query
- Batch insert para ETL
- Lazy initialization del query engine en Gradio

### Escalabilidad

- **ETL Incremental:** Solo procesar archivos nuevos
- **Filtros:** ETL por categoría/año/mes específico
- **Historial:** Queries guardadas en `qa_history`
- **Caché:** Gradio mantiene engine en memoria

## 📞 Soporte

Para problemas o preguntas:

1. Revisa la sección [Troubleshooting](#troubleshooting)
2. Ejecuta `python test_nexus.py --quick` para diagnóstico
3. Verifica logs en la consola con `--verbose`
4. Contacta al administrador del sistema

## 📄 Licencia

Este proyecto es para uso interno. Todos los derechos reservados.

---

**Desarrollado con ❤️ para análisis de datos de turismo en Andalucía**

*Versión: 1.0.0*
*Última actualización: Octubre 2025*
