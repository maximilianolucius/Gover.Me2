-- =============================================================================
-- RECREAR BASE DE DATOS NEXUS (versión corregida para permisos limitados)
-- =============================================================================
\set ON_ERROR_STOP on
\echo Iniciando configuración de base de datos 'nexus'...

-- Cambiar a la base de datos nexus directamente
\c nexus

-- Limpiar solo las tablas que podemos controlar (sin tocar el schema público)
-- Primero eliminamos las vistas
DROP VIEW IF EXISTS v_kpis_wide CASCADE;
DROP VIEW IF EXISTS v_datos_tipos_turismo CASCADE;
DROP VIEW IF EXISTS v_datos_mercados CASCADE;
DROP VIEW IF EXISTS v_datos_provinciales CASCADE;
DROP VIEW IF EXISTS v_datos_andalucia CASCADE;

-- Eliminar funciones
DROP FUNCTION IF EXISTS obtener_datos_periodo(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS buscar_indicador(TEXT) CASCADE;
DROP FUNCTION IF EXISTS buscar_mercado(TEXT) CASCADE;
DROP FUNCTION IF EXISTS buscar_provincia(TEXT) CASCADE;
DROP FUNCTION IF EXISTS insertar_dato_turismo(INTEGER, INTEGER, VARCHAR, DECIMAL, VARCHAR, VARCHAR, VARCHAR, DECIMAL, VARCHAR) CASCADE;

-- Eliminar tablas de alias
DROP TABLE IF EXISTS tipos_turismo_alias CASCADE;
DROP TABLE IF EXISTS indicadores_alias CASCADE;
DROP TABLE IF EXISTS provincias_alias CASCADE;
DROP TABLE IF EXISTS mercados_alias CASCADE;

-- Eliminar tabla principal y tablas de dimensión (estructura anterior)
DROP TABLE IF EXISTS datos_turismo CASCADE;
DROP TABLE IF EXISTS dim_fecha_mes CASCADE;
DROP TABLE IF EXISTS tipos_turismo CASCADE;
DROP TABLE IF EXISTS indicadores CASCADE;
DROP TABLE IF EXISTS mercados CASCADE;
DROP TABLE IF EXISTS provincias CASCADE;

-- *** ELIMINAR LAS 3 NUEVAS TABLAS ***
DROP TABLE IF EXISTS turismo_paises CASCADE;
DROP TABLE IF EXISTS turismo_espana CASCADE;
DROP TABLE IF EXISTS turismo_total CASCADE;

\echo Tablas anteriores eliminadas. Creando nueva estructura...

-- *** CREAR LAS 3 NUEVAS TABLAS ***

CREATE TABLE turismo_paises (
    año INT,
    mes INT,
    codigo_pais VARCHAR(3), -- 'FRA', 'DEU', 'GBR', etc.
    nombre_pais VARCHAR(50),
    viajeros_hoteles INT,
    pernoctaciones_hoteles BIGINT,

    -- Clave primaria compuesta
    PRIMARY KEY (año, mes, codigo_pais)
);

CREATE TABLE turismo_espana (
    año INT,
    mes INT,
    origen VARCHAR(20), -- 'andalucia', 'resto_espana', 'total_espana'
    viajeros_hoteles INT,
    pernoctaciones_hoteles BIGINT,
    llegadas_aeropuertos INT,
    turistas_millones DECIMAL(10,2),
    estancia_media_dias DECIMAL(4,1),
    gasto_medio_diario DECIMAL(8,2),

    -- Clave primaria compuesta
    PRIMARY KEY (año, mes, origen)
);

CREATE TABLE turismo_total (
    año INT,
    mes INT,
    categoria VARCHAR(20), -- 'total_andalucia', 'total_extranjeros', 'total_general', 'total_españa'
    viajeros_hoteles INT,
    pernoctaciones_hoteles BIGINT,
    llegadas_aeropuertos INT,
    turistas_millones DECIMAL(10,2),
    estancia_media_dias DECIMAL(4,1),
    gasto_medio_diario DECIMAL(8,2),

    PRIMARY KEY (año, mes, categoria)
);

-- Crear índices para optimizar consultas
CREATE INDEX idx_turismo_paises_fecha ON turismo_paises (año, mes);
CREATE INDEX idx_turismo_paises_pais ON turismo_paises (codigo_pais);
CREATE INDEX idx_turismo_espana_fecha ON turismo_espana (año, mes);
CREATE INDEX idx_turismo_espana_origen ON turismo_espana (origen);
CREATE INDEX idx_turismo_total_fecha ON turismo_total (año, mes);
CREATE INDEX idx_turismo_total_categoria ON turismo_total (categoria);

\echo Estructura de base de datos creada exitosamente.
\echo Tablas creadas: turismo_paises, turismo_espana, turismo_total
\echo Índices creados para optimizar consultas.