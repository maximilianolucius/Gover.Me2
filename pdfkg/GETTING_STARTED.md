# 🚀 Getting Started - Nexus

## Inicio Rápido en 5 Pasos

### 1️⃣ Instalar Dependencias

```bash
pip install -r requirements.txt
```

Esto instalará:
- pyarango (ArangoDB client)
- google-generativeai (Gemini API)
- openpyxl, pandas (procesamiento Excel)
- gradio (Web UI)
- plotly (visualizaciones)

### 2️⃣ Verificar ArangoDB

Asegúrate de que ArangoDB esté ejecutándose:

```bash
# Verificar
curl http://localhost:8529

# Si no está activo, iniciarlo
# En Linux/Mac:
sudo systemctl start arangodb3

# O ejecutar manualmente:
arangod
```

### 3️⃣ Verificar Configuración

Revisa que el archivo `.env` tiene tu API key de Gemini:

```bash
cat .env
```

Debe contener:
```
GEMINI_API_KEY=AIzaSy...  # Tu API key real
GEMINI_MODEL=gemini-2.0-flash-exp
ARANGO_HOST=localhost
ARANGO_PORT=8529
...
```

### 4️⃣ Ejecutar ETL

Procesa los archivos Excel y carga datos a la base de datos:

```bash
python nexus_cli.py etl
```

Esto tomará unos minutos. Verás progreso en pantalla.

**Salida esperada:**
```
✅ Archivos procesados: 126/126
📊 Métricas extraídas: 2,834
💾 Métricas cargadas: 2,834
⏱️  Duración: 123.45 segundos
```

### 5️⃣ ¡Empezar a Usar!

Elige una opción:

**A) Modo Chat Interactivo**
```bash
python nexus_cli.py chat
```

**B) Consulta Directa**
```bash
python nexus_cli.py query "¿Cuántos turistas británicos hubo en enero 2025?"
```

**C) Interfaz Web**
```bash
python nexus_app.py
```
Luego abre http://localhost:7860 en tu navegador

---

## 🎬 Script Automático

Para hacer todo automáticamente:

```bash
./quick_start.sh
```

Este script:
1. ✅ Verifica Python
2. ✅ Verifica ArangoDB
3. ✅ Verifica configuración
4. ✅ Instala dependencias (opcional)
5. ✅ Inicializa base de datos
6. ✅ Ejecuta ETL (opcional)
7. ✅ Ejecuta tests (opcional)
8. ✅ Lanza interfaz elegida

---

## 📝 Ejemplo Completo de Primera Sesión

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Verificar ArangoDB
curl http://localhost:8529
# ✅ Respuesta: {"error":false,"code":200,"version":"..."}

# 3. Ejecutar ETL
python nexus_cli.py etl
# ⏳ Procesando archivos...
# ✅ ETL completado: 2,834 métricas cargadas

# 4. Ver estadísticas
python nexus_cli.py stats
# 📊 Total métricas: 2,834
# 📅 Años cubiertos: 2023, 2024, 2025
# 📁 Categorías: 21

# 5. Hacer primera consulta
python nexus_cli.py query "¿Cuántos turistas británicos hubo en enero 2025?"
# 🤖 Respuesta:
# Según los datos de enero 2025, hubo aproximadamente 156,000 turistas
# británicos en Andalucía...

# 6. Iniciar modo chat
python nexus_cli.py chat
# 💬 MODO CHAT INTERACTIVO
# 👤 Tu pregunta: ¿Cómo varió el turismo de cruceros en 2024?
# 🤖 [Respuesta detallada...]
```

---

## 🧪 Verificar que Todo Funciona

Ejecuta la suite de tests:

```bash
python test_nexus.py
```

O solo una validación rápida:

```bash
python test_nexus.py --quick
```

**Todos los tests deben pasar:**
```
✅ PASS - Conexión a ArangoDB
✅ PASS - Parseo de nombres de archivo
✅ PASS - Clasificación de preguntas
✅ PASS - Extracción de parámetros
✅ PASS - Ejecución de queries AQL
✅ PASS - Consultas end-to-end
```

---

## ❓ Primeras Consultas Recomendadas

Una vez que el sistema esté funcionando, prueba estas consultas:

```
¿Cuántos turistas británicos hubo en enero 2025?
¿Qué provincia tuvo más turistas en 2024: Málaga o Sevilla?
¿Cómo varió el turismo de cruceros entre 2024 y 2025?
¿Cuál fue el gasto medio diario en el primer trimestre de 2024?
¿Cuántas pernoctaciones hubo en Granada en 2024?
```

---

## 🔧 Troubleshooting Común

### Error: "ModuleNotFoundError: No module named 'pyArango'"
**Solución:** Instala dependencias
```bash
pip install -r requirements.txt
```

### Error: "No se pudo conectar a ArangoDB"
**Solución:** Inicia ArangoDB
```bash
sudo systemctl start arangodb3
# o
arangod
```

### Error: "Gemini API Key not configured"
**Solución:** Verifica tu `.env`
```bash
# Edita el archivo
nano .env

# Asegúrate de que tiene:
GEMINI_API_KEY=tu_api_key_real_aqui
```

### ETL no encuentra archivos
**Solución:** Verifica directorio nexus/
```bash
ls nexus/*.xlsx | wc -l
# Debe mostrar número > 0
```

---

## 📚 Próximos Pasos

1. ✅ **Familiarízate con el sistema:** Prueba varias consultas
2. 📖 **Lee la documentación completa:** `NEXUS_README.md`
3. 📝 **Revisa ejemplos de queries:** `QUERIES_EXAMPLES.md`
4. 🌐 **Explora la interfaz web:** `python nexus_app.py`
5. 🔄 **Actualiza datos regularmente:** Ejecuta ETL cuando lleguen datos nuevos

---

## 🎯 Comandos Más Usados

```bash
# Consultas
python nexus_cli.py chat                    # Modo interactivo
python nexus_cli.py query "tu pregunta"     # Consulta única
python nexus_app.py                         # Interfaz web

# Gestión de datos
python nexus_cli.py etl                     # Cargar todos los datos
python nexus_cli.py etl --year 2025         # Solo datos de 2025
python nexus_cli.py stats                   # Ver estadísticas

# Mantenimiento
python test_nexus.py                        # Ejecutar tests
python nexus_cli.py clear --confirm         # Limpiar datos
```

---

## 📞 Ayuda

- **Documentación completa:** `NEXUS_README.md`
- **Ejemplos de queries:** `QUERIES_EXAMPLES.md`
- **Resumen del proyecto:** `PROJECT_SUMMARY.md`
- **Este guía:** `GETTING_STARTED.md`

**Comandos de ayuda:**
```bash
python nexus_cli.py --help
python nexus_cli.py etl --help
python nexus_cli.py query --help
```

---

¡Listo! Ya estás preparado para empezar a usar Nexus 🚀
