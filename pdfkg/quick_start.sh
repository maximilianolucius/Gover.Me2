#!/bin/bash

# Quick Start Script para Nexus - Sistema de Análisis de Turismo
# Este script ayuda a configurar e inicializar el sistema rápidamente

set -e  # Exit on error

echo "=================================="
echo "🚀 NEXUS QUICK START"
echo "=================================="
echo ""

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Función para imprimir mensajes de éxito
success() {
    echo -e "${GREEN}✅ $1${NC}"
}

# Función para imprimir mensajes de error
error() {
    echo -e "${RED}❌ $1${NC}"
}

# Función para imprimir mensajes de info
info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# 1. Verificar Python
info "Verificando Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    success "Python $PYTHON_VERSION encontrado"
else
    error "Python 3 no encontrado. Por favor instálalo primero."
    exit 1
fi

# 2. Verificar ArangoDB
info "Verificando ArangoDB..."
if curl -s http://localhost:8529 &> /dev/null; then
    success "ArangoDB está ejecutándose en localhost:8529"
else
    error "ArangoDB no está ejecutándose o no es accesible"
    error "Por favor, inicia ArangoDB antes de continuar"
    exit 1
fi

# 3. Verificar archivo .env
info "Verificando configuración..."
if [ -f .env ]; then
    if grep -q "GEMINI_API_KEY=AI" .env; then
        success "Archivo .env encontrado con API key configurada"
    else
        error "API key de Gemini no configurada en .env"
        info "Por favor, edita el archivo .env y agrega tu GEMINI_API_KEY"
        exit 1
    fi
else
    error "Archivo .env no encontrado"
    exit 1
fi

# 4. Instalar dependencias
info "Verificando dependencias..."
if [ -f requirements.txt ]; then
    echo ""
    read -p "¿Instalar/actualizar dependencias? (s/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Ss]$ ]]; then
        info "Instalando dependencias..."
        pip install -q -r requirements.txt
        success "Dependencias instaladas"
    fi
else
    error "requirements.txt no encontrado"
    exit 1
fi

# 5. Verificar archivos de datos
info "Verificando archivos de datos..."
EXCEL_COUNT=$(ls nexus/*.xlsx 2>/dev/null | wc -l)
if [ $EXCEL_COUNT -gt 0 ]; then
    success "$EXCEL_COUNT archivos Excel encontrados en nexus/"
else
    error "No se encontraron archivos Excel en nexus/"
    error "Por favor, coloca los archivos .xlsx en el directorio nexus/"
    exit 1
fi

# 6. Inicializar base de datos
echo ""
info "Inicializando base de datos..."
python3 nexus_db.py
if [ $? -eq 0 ]; then
    success "Base de datos inicializada"
else
    error "Error al inicializar la base de datos"
    exit 1
fi

# 7. Preguntar si ejecutar ETL
echo ""
echo "=================================="
echo "📊 ETL Pipeline"
echo "=================================="
info "El ETL procesará todos los archivos Excel y cargará datos en la DB"
info "Esto puede tomar varios minutos dependiendo del número de archivos"
echo ""
read -p "¿Ejecutar ETL ahora? (s/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Ss]$ ]]; then
    info "Ejecutando ETL..."
    python3 nexus_cli.py etl
    if [ $? -eq 0 ]; then
        success "ETL completado exitosamente"
    else
        error "Error durante el ETL"
        exit 1
    fi
else
    info "ETL omitido. Puedes ejecutarlo más tarde con: python nexus_cli.py etl"
fi

# 8. Mostrar estadísticas
echo ""
info "Obteniendo estadísticas de la base de datos..."
python3 nexus_cli.py stats

# 9. Ejecutar tests básicos
echo ""
echo "=================================="
echo "🧪 Tests Básicos"
echo "=================================="
read -p "¿Ejecutar tests de validación? (s/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Ss]$ ]]; then
    info "Ejecutando tests..."
    python3 test_nexus.py
    if [ $? -eq 0 ]; then
        success "Todos los tests pasaron"
    else
        error "Algunos tests fallaron"
    fi
fi

# 10. Menú de opciones
echo ""
echo "=================================="
echo "✅ SISTEMA LISTO"
echo "=================================="
echo ""
echo "¿Qué deseas hacer ahora?"
echo ""
echo "1) Iniciar modo chat interactivo"
echo "2) Lanzar interfaz web (Gradio)"
echo "3) Hacer una consulta de prueba"
echo "4) Ver ayuda y ejemplos"
echo "5) Salir"
echo ""
read -p "Selecciona una opción (1-5): " option

case $option in
    1)
        info "Iniciando modo chat..."
        python3 nexus_cli.py chat
        ;;
    2)
        info "Lanzando interfaz web..."
        info "Accede a http://localhost:7860 en tu navegador"
        python3 nexus_app.py
        ;;
    3)
        info "Ejecutando consulta de prueba..."
        python3 nexus_cli.py query "¿Cuántos turistas británicos hubo en enero 2025?"
        ;;
    4)
        info "Mostrando ayuda..."
        python3 nexus_cli.py --help
        echo ""
        info "Ver más ejemplos en: QUERIES_EXAMPLES.md"
        ;;
    5)
        success "¡Hasta luego!"
        exit 0
        ;;
    *)
        error "Opción no válida"
        ;;
esac

echo ""
success "Configuración completada"
echo ""
echo "📚 Comandos útiles:"
echo "  • python nexus_cli.py chat          - Modo chat interactivo"
echo "  • python nexus_app.py               - Lanzar interfaz web"
echo "  • python nexus_cli.py stats         - Ver estadísticas"
echo "  • python nexus_cli.py query \"...\"   - Hacer una consulta"
echo ""
echo "📖 Documentación completa: NEXUS_README.md"
echo ""
