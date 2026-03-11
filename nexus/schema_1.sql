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

-- Eliminar tabla principal y tablas de dimensión
DROP TABLE IF EXISTS datos_turismo CASCADE;
DROP TABLE IF EXISTS dim_fecha_mes CASCADE;
DROP TABLE IF EXISTS tipos_turismo CASCADE;
DROP TABLE IF EXISTS indicadores CASCADE;
DROP TABLE IF EXISTS mercados CASCADE;
DROP TABLE IF EXISTS provincias CASCADE;

\echo Tablas anteriores eliminadas. Creando nueva estructura...

-- ESQUEMA DE BASE DE DATOS PARA SISTEMA DE DATOS TURÍSTICOS DE ANDALUCÍA
-- PostgreSQL Schema para datos mensuales 2021-2025

-- =============================================================================
-- TABLAS DE DIMENSIONES (CATÁLOGOS)
-- =============================================================================

-- Tabla de Provincias de Andalucía
CREATE TABLE provincias (
    id_provincia SERIAL PRIMARY KEY,
    codigo_provincia VARCHAR(2) UNIQUE NOT NULL, -- Código INE: 04, 11, 14, 18, 21, 23, 29, 41
    nombre_provincia VARCHAR(50) NOT NULL UNIQUE, -- Almería, Cádiz, Córdoba, Granada, Huelva, Jaén, Málaga, Sevilla
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Mercados Turísticos (Segmentación por origen)
CREATE TABLE mercados (
    id_mercado SERIAL PRIMARY KEY,
    codigo_mercado VARCHAR(20) UNIQUE NOT NULL, -- alemanes, britanicos, extranjeros, resto_espana, andaluces, espanoles, otros_mercados
    nombre_mercado VARCHAR(50) NOT NULL,
    tipo_mercado VARCHAR(20) NOT NULL, -- 'nacional', 'internacional', 'regional'
    descripcion TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Tipos de Turismo
CREATE TABLE tipos_turismo (
    id_tipo_turismo SERIAL PRIMARY KEY,
    codigo_tipo VARCHAR(20) UNIQUE NOT NULL, -- cultural, ciudad, cruceros, interior, litoral
    nombre_tipo VARCHAR(50) NOT NULL,
    descripcion TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Indicadores Turísticos
CREATE TABLE indicadores (
    id_indicador SERIAL PRIMARY KEY,
    codigo_indicador VARCHAR(50) UNIQUE NOT NULL,
    nombre_indicador VARCHAR(200) NOT NULL,
    unidad_medida VARCHAR(50), -- 'viajeros', 'pernoctaciones', 'porcentaje', 'euros', 'dias', 'puntos'
    categoria_indicador VARCHAR(50), -- 'alojamiento', 'transporte', 'gasto', 'valoracion', 'infraestructura'
    descripcion TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- TABLA PRINCIPAL DE DATOS TURÍSTICOS
-- =============================================================================

CREATE TABLE datos_turismo (
    id_dato BIGSERIAL PRIMARY KEY,

    -- Dimensiones temporales
    anio INTEGER NOT NULL CHECK (anio >= 2021 AND anio <= 2030),
    mes INTEGER NOT NULL CHECK (mes >= 1 AND mes <= 12),
    fecha_periodo DATE NOT NULL, -- Primer día del mes para facilitar consultas

    -- Dimensiones geográficas y de segmentación
    id_provincia INTEGER REFERENCES provincias(id_provincia),
    id_mercado INTEGER REFERENCES mercados(id_mercado),
    id_tipo_turismo INTEGER REFERENCES tipos_turismo(id_tipo_turismo),

    -- Indicador y valores
    id_indicador INTEGER NOT NULL REFERENCES indicadores(id_indicador),
    valor_absoluto DECIMAL(15,3), -- Valor principal del indicador
    variacion_interanual DECIMAL(8,6), -- Variación respecto al mismo período del año anterior

    -- Contexto del dato
    ambito_geografico VARCHAR(20) NOT NULL DEFAULT 'provincia', -- 'andalucia', 'provincia'
    tipo_agregacion VARCHAR(20) NOT NULL DEFAULT 'mensual', -- 'mensual', 'acumulado'

    -- Metadatos
    fuente_dato VARCHAR(100) DEFAULT 'Sistema de Información Turística de Andalucía',
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archivo_origen VARCHAR(100), -- Referencia al archivo Excel original
    observaciones TEXT,

    -- Columnas generadas para resolver problema de NULLs en UNIQUE constraints
    prov_key INTEGER GENERATED ALWAYS AS (COALESCE(id_provincia, 0)) STORED,
    mercado_key INTEGER GENERATED ALWAYS AS (COALESCE(id_mercado, 0)) STORED,
    tipo_key INTEGER GENERATED ALWAYS AS (COALESCE(id_tipo_turismo, 0)) STORED,

    -- Clave de fecha amigable para consultas (formato: YYYYMM)
    date_id INTEGER GENERATED ALWAYS AS (anio * 100 + mes) STORED,

    -- Constraint UNIQUE que maneja correctamente los NULLs
    CONSTRAINT uk_dato_unique_fix UNIQUE (anio, mes, prov_key, mercado_key, tipo_key, id_indicador, ambito_geografico),

    -- Validaciones
    CONSTRAINT ck_fecha_consistente CHECK (
        fecha_periodo = DATE(anio || '-' || LPAD(mes::TEXT, 2, '0') || '-01')
    )
);

-- =============================================================================
-- TABLA DE DIMENSIÓN DE FECHAS (para facilitar consultas temporales)
-- =============================================================================

CREATE TABLE dim_fecha_mes (
    date_id INTEGER PRIMARY KEY,         -- 202501 = enero 2025
    fecha_periodo DATE NOT NULL UNIQUE,  -- 2025-01-01
    anio SMALLINT NOT NULL,
    mes SMALLINT NOT NULL,
    mes_nombre_es TEXT NOT NULL,         -- 'enero', 'febrero', etc.
    mes_nombre_abrev TEXT NOT NULL,      -- 'ene', 'feb', etc.
    trimestre SMALLINT NOT NULL,         -- 1, 2, 3, 4
    semestre SMALLINT NOT NULL,          -- 1, 2
    CHECK (anio >= 2021 AND anio <= 2030),
    CHECK (mes >= 1 AND mes <= 12),
    CHECK (trimestre >= 1 AND trimestre <= 4),
    CHECK (semestre >= 1 AND semestre <= 2)
);

-- Datos iniciales para dim_fecha_mes (2021-2025)
INSERT INTO dim_fecha_mes (date_id, fecha_periodo, anio, mes, mes_nombre_es, mes_nombre_abrev, trimestre, semestre)
SELECT
    anio * 100 + mes as date_id,
    DATE(anio || '-' || LPAD(mes::TEXT, 2, '0') || '-01') as fecha_periodo,
    anio,
    mes,
    CASE mes
        WHEN 1 THEN 'enero' WHEN 2 THEN 'febrero' WHEN 3 THEN 'marzo'
        WHEN 4 THEN 'abril' WHEN 5 THEN 'mayo' WHEN 6 THEN 'junio'
        WHEN 7 THEN 'julio' WHEN 8 THEN 'agosto' WHEN 9 THEN 'septiembre'
        WHEN 10 THEN 'octubre' WHEN 11 THEN 'noviembre' WHEN 12 THEN 'diciembre'
    END as mes_nombre_es,
    CASE mes
        WHEN 1 THEN 'ene' WHEN 2 THEN 'feb' WHEN 3 THEN 'mar'
        WHEN 4 THEN 'abr' WHEN 5 THEN 'may' WHEN 6 THEN 'jun'
        WHEN 7 THEN 'jul' WHEN 8 THEN 'ago' WHEN 9 THEN 'sep'
        WHEN 10 THEN 'oct' WHEN 11 THEN 'nov' WHEN 12 THEN 'dic'
    END as mes_nombre_abrev,
    CASE
        WHEN mes IN (1,2,3) THEN 1
        WHEN mes IN (4,5,6) THEN 2
        WHEN mes IN (7,8,9) THEN 3
        WHEN mes IN (10,11,12) THEN 4
    END as trimestre,
    CASE WHEN mes <= 6 THEN 1 ELSE 2 END as semestre
FROM generate_series(2021, 2025) anio
CROSS JOIN generate_series(1, 12) mes;

-- =============================================================================
-- VISTA PARA DATOS TOTALES DE ANDALUCÍA
-- =============================================================================

CREATE VIEW v_datos_andalucia AS
SELECT
    dt.anio,
    dt.mes,
    dt.fecha_periodo,
    i.codigo_indicador,
    i.nombre_indicador,
    i.unidad_medida,
    dt.valor_absoluto,
    dt.variacion_interanual,
    dt.fecha_actualizacion
FROM datos_turismo dt
JOIN indicadores i ON dt.id_indicador = i.id_indicador
WHERE dt.ambito_geografico = 'andalucia'
    AND dt.id_provincia IS NULL
    AND dt.id_mercado IS NULL
    AND dt.id_tipo_turismo IS NULL;

-- =============================================================================
-- VISTA PARA DATOS POR PROVINCIA
-- =============================================================================

CREATE VIEW v_datos_provinciales AS
SELECT
    dt.anio,
    dt.mes,
    dt.fecha_periodo,
    p.codigo_provincia,
    p.nombre_provincia,
    i.codigo_indicador,
    i.nombre_indicador,
    i.unidad_medida,
    dt.valor_absoluto,
    dt.variacion_interanual,
    dt.fecha_actualizacion
FROM datos_turismo dt
JOIN provincias p ON dt.id_provincia = p.id_provincia
JOIN indicadores i ON dt.id_indicador = i.id_indicador
WHERE dt.ambito_geografico = 'provincia';

-- =============================================================================
-- VISTA PARA DATOS POR MERCADO TURÍSTICO
-- =============================================================================

CREATE VIEW v_datos_mercados AS
SELECT
    dt.anio,
    dt.mes,
    dt.fecha_periodo,
    m.codigo_mercado,
    m.nombre_mercado,
    m.tipo_mercado,
    i.codigo_indicador,
    i.nombre_indicador,
    i.unidad_medida,
    dt.valor_absoluto,
    dt.variacion_interanual,
    dt.fecha_actualizacion
FROM datos_turismo dt
JOIN mercados m ON dt.id_mercado = m.id_mercado
JOIN indicadores i ON dt.id_indicador = i.id_indicador
WHERE dt.id_mercado IS NOT NULL;

-- =============================================================================
-- VISTA PARA DATOS POR TIPO DE TURISMO
-- =============================================================================

CREATE VIEW v_datos_tipos_turismo AS
SELECT
    dt.anio,
    dt.mes,
    dt.fecha_periodo,
    tt.codigo_tipo,
    tt.nombre_tipo,
    i.codigo_indicador,
    i.nombre_indicador,
    i.unidad_medida,
    dt.valor_absoluto,
    dt.variacion_interanual,
    dt.fecha_actualizacion
FROM datos_turismo dt
JOIN tipos_turismo tt ON dt.id_tipo_turismo = tt.id_tipo_turismo
JOIN indicadores i ON dt.id_indicador = i.id_indicador
WHERE dt.id_tipo_turismo IS NOT NULL;

-- =============================================================================
-- VISTA ANCHA CON KPIS PRINCIPALES (para análisis más fácil)
-- =============================================================================

CREATE OR REPLACE VIEW v_kpis_wide AS
SELECT
    dt.anio,
    dt.mes,
    dt.fecha_periodo,
    dt.date_id,
    df.mes_nombre_es,
    df.trimestre,
    df.semestre,
    p.codigo_provincia,
    p.nombre_provincia,
    m.codigo_mercado,
    m.nombre_mercado,
    m.tipo_mercado,
    tt.codigo_tipo,
    tt.nombre_tipo,

    -- KPIs principales como columnas
    MAX(CASE WHEN i.codigo_indicador = 'viajeros_hoteles_total' THEN dt.valor_absoluto END) AS viajeros_total,
    MAX(CASE WHEN i.codigo_indicador = 'viajeros_hoteles_espanoles' THEN dt.valor_absoluto END) AS viajeros_espanoles,
    MAX(CASE WHEN i.codigo_indicador = 'viajeros_hoteles_extranjeros' THEN dt.valor_absoluto END) AS viajeros_extranjeros,
    MAX(CASE WHEN i.codigo_indicador = 'pernoctaciones_hoteles_total' THEN dt.valor_absoluto END) AS pernoctaciones_total,
    MAX(CASE WHEN i.codigo_indicador = 'pernoctaciones_hoteles_espanolas' THEN dt.valor_absoluto END) AS pernoctaciones_espanolas,
    MAX(CASE WHEN i.codigo_indicador = 'pernoctaciones_hoteles_extranjeras' THEN dt.valor_absoluto END) AS pernoctaciones_extranjeras,
    MAX(CASE WHEN i.codigo_indicador = 'ocupacion_hotelera' THEN dt.valor_absoluto END) AS ocupacion_hotelera,
    MAX(CASE WHEN i.codigo_indicador = 'estancia_media' THEN dt.valor_absoluto END) AS estancia_media,
    MAX(CASE WHEN i.codigo_indicador = 'gasto_medio_diario' THEN dt.valor_absoluto END) AS gasto_medio_diario,
    MAX(CASE WHEN i.codigo_indicador = 'numero_turistas' THEN dt.valor_absoluto END) AS numero_turistas,
    MAX(CASE WHEN i.codigo_indicador = 'valoracion_destino' THEN dt.valor_absoluto END) AS valoracion_destino,
    MAX(CASE WHEN i.codigo_indicador = 'llegadas_aeropuerto_total' THEN dt.valor_absoluto END) AS llegadas_aeropuerto,
    MAX(CASE WHEN i.codigo_indicador = 'establecimientos_hoteles' THEN dt.valor_absoluto END) AS establecimientos_hoteles,
    MAX(CASE WHEN i.codigo_indicador = 'plazas_hoteles' THEN dt.valor_absoluto END) AS plazas_hoteles,

    -- Variaciones interanuales de KPIs principales
    MAX(CASE WHEN i.codigo_indicador = 'viajeros_hoteles_total' THEN dt.variacion_interanual END) AS var_viajeros_total,
    MAX(CASE WHEN i.codigo_indicador = 'pernoctaciones_hoteles_total' THEN dt.variacion_interanual END) AS var_pernoctaciones_total,
    MAX(CASE WHEN i.codigo_indicador = 'numero_turistas' THEN dt.variacion_interanual END) AS var_numero_turistas

FROM datos_turismo dt
LEFT JOIN dim_fecha_mes df ON dt.date_id = df.date_id
LEFT JOIN provincias p     ON dt.id_provincia = p.id_provincia
LEFT JOIN mercados m       ON dt.id_mercado = m.id_mercado
LEFT JOIN tipos_turismo tt ON dt.id_tipo_turismo = tt.id_tipo_turismo
LEFT JOIN indicadores i    ON dt.id_indicador = i.id_indicador
GROUP BY
    dt.anio, dt.mes, dt.fecha_periodo, dt.date_id,
    df.mes_nombre_es, df.trimestre, df.semestre,
    p.codigo_provincia, p.nombre_provincia,
    m.codigo_mercado, m.nombre_mercado, m.tipo_mercado,
    tt.codigo_tipo, tt.nombre_tipo;

-- =============================================================================
-- TABLAS DE ALIAS PARA BÚSQUEDAS SEMÁNTICAS
-- =============================================================================

-- Alias para mercados turísticos
CREATE TABLE mercados_alias (
    id SERIAL PRIMARY KEY,
    id_mercado INTEGER NOT NULL REFERENCES mercados(id_mercado),
    alias TEXT NOT NULL,
    lang TEXT DEFAULT 'es',
    descripcion TEXT
);

-- Alias para provincias
CREATE TABLE provincias_alias (
    id SERIAL PRIMARY KEY,
    id_provincia INTEGER NOT NULL REFERENCES provincias(id_provincia),
    alias TEXT NOT NULL,
    lang TEXT DEFAULT 'es',
    descripcion TEXT
);

-- Alias para indicadores
CREATE TABLE indicadores_alias (
    id SERIAL PRIMARY KEY,
    id_indicador INTEGER NOT NULL REFERENCES indicadores(id_indicador),
    alias TEXT NOT NULL,
    lang TEXT DEFAULT 'es',
    descripcion TEXT
);

-- Alias para tipos de turismo
CREATE TABLE tipos_turismo_alias (
    id SERIAL PRIMARY KEY,
    id_tipo_turismo INTEGER NOT NULL REFERENCES tipos_turismo(id_tipo_turismo),
    alias TEXT NOT NULL,
    lang TEXT DEFAULT 'es',
    descripcion TEXT
);

-- =============================================================================
-- ÍNDICES PARA OPTIMIZACIÓN DE CONSULTAS
-- =============================================================================

-- Índices temporales
CREATE INDEX idx_datos_turismo_fecha ON datos_turismo(anio, mes);
CREATE INDEX idx_datos_turismo_fecha_periodo ON datos_turismo(fecha_periodo);
CREATE INDEX idx_datos_date_id ON datos_turismo(date_id); -- Nuevo índice para consultas con date_id

-- Índices por dimensiones
CREATE INDEX idx_datos_turismo_provincia ON datos_turismo(id_provincia);
CREATE INDEX idx_datos_turismo_mercado ON datos_turismo(id_mercado);
CREATE INDEX idx_datos_turismo_tipo_turismo ON datos_turismo(id_tipo_turismo);
CREATE INDEX idx_datos_turismo_indicador ON datos_turismo(id_indicador);

-- Índices compuestos para consultas frecuentes
CREATE INDEX idx_datos_provincia_fecha ON datos_turismo(id_provincia, anio, mes);
CREATE INDEX idx_datos_indicador_fecha ON datos_turismo(id_indicador, anio, mes);
CREATE INDEX idx_datos_mercado_fecha ON datos_turismo(id_mercado, anio, mes);
CREATE INDEX idx_datos_date_id_provincia ON datos_turismo(date_id, id_provincia);
CREATE INDEX idx_datos_date_id_indicador ON datos_turismo(date_id, id_indicador);

-- Índices para las columnas generadas (claves para UNIQUE constraint)
CREATE INDEX idx_datos_prov_key ON datos_turismo(prov_key);
CREATE INDEX idx_datos_mercado_key ON datos_turismo(mercado_key);
CREATE INDEX idx_datos_tipo_key ON datos_turismo(tipo_key);

-- Índices únicos para garantizar unicidad por alias sin sensibilidad a mayúsculas/minúsculas
CREATE UNIQUE INDEX ux_mercados_alias_id_lower_alias
    ON mercados_alias (id_mercado, LOWER(alias));
CREATE UNIQUE INDEX ux_provincias_alias_id_lower_alias
    ON provincias_alias (id_provincia, LOWER(alias));
CREATE UNIQUE INDEX ux_indicadores_alias_id_lower_alias
    ON indicadores_alias (id_indicador, LOWER(alias));
CREATE UNIQUE INDEX ux_tipos_turismo_alias_id_lower_alias
    ON tipos_turismo_alias (id_tipo_turismo, LOWER(alias));

-- =============================================================================
-- FUNCIONES DE UTILIDAD
-- =============================================================================

-- Función para insertar datos desde archivos Excel (CORREGIDA)
CREATE OR REPLACE FUNCTION insertar_dato_turismo(
    p_anio INTEGER,
    p_mes INTEGER,
    p_codigo_indicador VARCHAR,
    p_valor DECIMAL,
    p_provincia VARCHAR DEFAULT NULL,
    p_mercado VARCHAR DEFAULT NULL,
    p_tipo_turismo VARCHAR DEFAULT NULL,
    p_variacion DECIMAL DEFAULT NULL,
    p_archivo_origen VARCHAR DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_id_provincia INTEGER;
    v_id_mercado INTEGER;
    v_id_tipo_turismo INTEGER;
    v_id_indicador INTEGER;
    v_fecha_periodo DATE;
    v_id_dato BIGINT;
BEGIN
    -- Calcular fecha del período
    v_fecha_periodo := DATE(p_anio || '-' || LPAD(p_mes::TEXT, 2, '0') || '-01');

    -- Obtener IDs de las dimensiones
    IF p_provincia IS NOT NULL THEN
        SELECT id_provincia INTO v_id_provincia
        FROM provincias WHERE codigo_provincia = p_provincia;
    END IF;

    IF p_mercado IS NOT NULL THEN
        SELECT id_mercado INTO v_id_mercado
        FROM mercados WHERE codigo_mercado = p_mercado;
    END IF;

    IF p_tipo_turismo IS NOT NULL THEN
        SELECT id_tipo_turismo INTO v_id_tipo_turismo
        FROM tipos_turismo WHERE codigo_tipo = p_tipo_turismo;
    END IF;

    SELECT id_indicador INTO v_id_indicador
    FROM indicadores WHERE codigo_indicador = p_codigo_indicador;

    -- Insertar el dato
    INSERT INTO datos_turismo (
        anio, mes, fecha_periodo,
        id_provincia, id_mercado, id_tipo_turismo, id_indicador,
        valor_absoluto, variacion_interanual, archivo_origen
    ) VALUES (
        p_anio, p_mes, v_fecha_periodo,
        v_id_provincia, v_id_mercado, v_id_tipo_turismo, v_id_indicador,
        p_valor, p_variacion, p_archivo_origen
    )
    ON CONFLICT ON CONSTRAINT uk_dato_unique_fix
    DO UPDATE SET
        valor_absoluto = EXCLUDED.valor_absoluto,
        variacion_interanual = EXCLUDED.variacion_interanual,
        fecha_actualizacion = CURRENT_TIMESTAMP,
        archivo_origen = EXCLUDED.archivo_origen
    RETURNING id_dato INTO v_id_dato;

    RETURN v_id_dato;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FUNCIONES ADICIONALES PARA TRABAJO CON ALIAS
-- =============================================================================

-- Función para buscar provincia por alias
CREATE OR REPLACE FUNCTION buscar_provincia(p_alias TEXT)
RETURNS TABLE(id_provincia INTEGER, codigo_provincia VARCHAR, nombre_provincia VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT p.id_provincia, p.codigo_provincia, p.nombre_provincia
    FROM provincias p
    LEFT JOIN provincias_alias pa ON p.id_provincia = pa.id_provincia
    WHERE LOWER(p.nombre_provincia) = LOWER(p_alias)
       OR LOWER(p.codigo_provincia) = LOWER(p_alias)
       OR LOWER(pa.alias) = LOWER(p_alias);
END;
$$ LANGUAGE plpgsql;

-- Función para buscar mercado por alias
CREATE OR REPLACE FUNCTION buscar_mercado(p_alias TEXT)
RETURNS TABLE(id_mercado INTEGER, codigo_mercado VARCHAR, nombre_mercado VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT m.id_mercado, m.codigo_mercado, m.nombre_mercado
    FROM mercados m
    LEFT JOIN mercados_alias ma ON m.id_mercado = ma.id_mercado
    WHERE LOWER(m.nombre_mercado) = LOWER(p_alias)
       OR LOWER(m.codigo_mercado) = LOWER(p_alias)
       OR LOWER(ma.alias) = LOWER(p_alias);
END;
$$ LANGUAGE plpgsql;

-- Función para buscar indicador por alias
CREATE OR REPLACE FUNCTION buscar_indicador(p_alias TEXT)
RETURNS TABLE(id_indicador INTEGER, codigo_indicador VARCHAR, nombre_indicador VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT i.id_indicador, i.codigo_indicador, i.nombre_indicador
    FROM indicadores i
    LEFT JOIN indicadores_alias ia ON i.id_indicador = ia.id_indicador
    WHERE LOWER(i.nombre_indicador) LIKE '%' || LOWER(p_alias) || '%'
       OR LOWER(i.codigo_indicador) = LOWER(p_alias)
       OR LOWER(ia.alias) = LOWER(p_alias);
END;
$$ LANGUAGE plpgsql;

-- Función para obtener datos de un período específico (usando date_id)
CREATE OR REPLACE FUNCTION obtener_datos_periodo(p_date_id INTEGER)
RETURNS TABLE(
    provincia VARCHAR, mercado VARCHAR, tipo_turismo VARCHAR, indicador VARCHAR,
    valor DECIMAL, variacion DECIMAL, fecha DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE(p.nombre_provincia, 'Total Andalucía')::VARCHAR as provincia,
        COALESCE(m.nombre_mercado, 'Todos los mercados')::VARCHAR as mercado,
        COALESCE(tt.nombre_tipo, 'Todos los tipos')::VARCHAR as tipo_turismo,
        i.nombre_indicador::VARCHAR as indicador,
        dt.valor_absoluto as valor,
        dt.variacion_interanual as variacion,
        dt.fecha_periodo as fecha
    FROM datos_turismo dt
    JOIN indicadores i ON dt.id_indicador = i.id_indicador
    LEFT JOIN provincias p ON dt.id_provincia = p.id_provincia
    LEFT JOIN mercados m ON dt.id_mercado = m.id_mercado
    LEFT JOIN tipos_turismo tt ON dt.id_tipo_turismo = tt.id_tipo_turismo
    WHERE dt.date_id = p_date_id
    ORDER BY provincia, mercado, tipo_turismo, indicador;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- DATOS INICIALES (CATÁLOGOS)
-- =============================================================================

-- Insertar provincias
INSERT INTO provincias (codigo_provincia, nombre_provincia) VALUES
('04', 'Almería'),
('11', 'Cádiz'),
('14', 'Córdoba'),
('18', 'Granada'),
('21', 'Huelva'),
('23', 'Jaén'),
('29', 'Málaga'),
('41', 'Sevilla');

-- Insertar mercados turísticos
INSERT INTO mercados (codigo_mercado, nombre_mercado, tipo_mercado, descripcion) VALUES
('alemanes', 'Mercado Alemán', 'internacional', 'Turistas procedentes de Alemania'),
('britanicos', 'Mercado Británico', 'internacional', 'Turistas procedentes del Reino Unido'),
('extranjeros', 'Mercado Extranjero Total', 'internacional', 'Total de turistas extranjeros'),
('resto_espana', 'Resto de España', 'nacional', 'Turistas españoles no andaluces'),
('andaluces', 'Mercado Andaluz', 'regional', 'Turistas procedentes de Andalucía'),
('espanoles', 'Mercado Español Total', 'nacional', 'Total de turistas españoles'),
('otros_mercados', 'Otros Mercados', 'mixto', 'Otros mercados internacionales');

-- Insertar tipos de turismo
INSERT INTO tipos_turismo (codigo_tipo, nombre_tipo, descripcion) VALUES
('cultural', 'Turismo Cultural', 'Turismo enfocado en patrimonio y cultura'),
('ciudad', 'Turismo de Ciudad', 'Turismo urbano y de ciudades'),
('cruceros', 'Turismo de Cruceros', 'Turismo marítimo de cruceros'),
('interior', 'Turismo de Interior', 'Turismo en zonas rurales e interiores'),
('litoral', 'Turismo de Litoral', 'Turismo costero y de playa');

-- Insertar indicadores principales (basados en el análisis de los archivos)
INSERT INTO indicadores (codigo_indicador, nombre_indicador, unidad_medida, categoria_indicador) VALUES
-- Alojamiento hotelero
('viajeros_hoteles_total', 'Número de viajeros en establecimientos hoteleros', 'viajeros', 'alojamiento'),
('viajeros_hoteles_espanoles', 'Número de viajeros españoles en establecimientos hoteleros', 'viajeros', 'alojamiento'),
('viajeros_hoteles_extranjeros', 'Número de viajeros extranjeros en establecimientos hoteleros', 'viajeros', 'alojamiento'),
('pernoctaciones_hoteles_total', 'Número de pernoctaciones en establecimientos hoteleros', 'pernoctaciones', 'alojamiento'),
('pernoctaciones_hoteles_espanolas', 'Número de pernoctaciones españolas en establecimientos hoteleros', 'pernoctaciones', 'alojamiento'),
('pernoctaciones_hoteles_extranjeras', 'Número de pernoctaciones extranjeras en establecimientos hoteleros', 'pernoctaciones', 'alojamiento'),
('cuota_pernoctaciones_andalucia', 'Cuota (% sobre total pernoctaciones en Andalucía)', 'porcentaje', 'alojamiento'),

-- Infraestructura hotelera
('establecimientos_hoteles', 'Establecimientos hoteleros', 'establecimientos', 'infraestructura'),
('plazas_hoteles', 'Plazas en establecimientos hoteleros', 'plazas', 'infraestructura'),
('ocupacion_hotelera', 'Grado de ocupación hotelera', 'porcentaje', 'alojamiento'),
('personal_empleado_hoteles', 'Personal empleado en establecimientos hoteleros', 'personas', 'infraestructura'),

-- Transporte aéreo
('llegadas_aeropuerto_total', 'Llegadas de pasajeros al aeropuerto. Total', 'pasajeros', 'transporte'),
('llegadas_aeropuerto_nacionales', 'Llegadas de pasajeros al aeropuerto. Nacionales', 'pasajeros', 'transporte'),
('llegadas_aeropuerto_internacionales', 'Llegadas de pasajeros al aeropuerto. Internacionales', 'pasajeros', 'transporte'),

-- Turistas y gasto
('numero_turistas', 'Número de turistas', 'turistas', 'turistas'),
('cuota_turistas_andalucia', 'Cuota (% sobre total turistas en Andalucía)', 'porcentaje', 'turistas'),
('estancia_media', 'Estancia Media (número de días)', 'dias', 'turistas'),
('gasto_medio_diario', 'Gasto medio diario (euros)', 'euros', 'gasto'),
('valoracion_destino', 'Valoración del destino: escala de 1 a 10', 'puntos', 'valoracion');

-- =============================================================================
-- DATOS INICIALES PARA TABLAS DE ALIAS
-- =============================================================================

-- Alias para provincias (códigos, abreviaciones, nombres alternativos)
INSERT INTO provincias_alias (id_provincia, alias, lang, descripcion) VALUES
-- Almería
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '04'), 'almeria', 'es', 'Nombre sin tilde'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '04'), 'alm', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '04'), '04', 'es', 'Código INE'),
-- Cádiz
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '11'), 'cadiz', 'es', 'Nombre sin tilde'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '11'), 'cad', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '11'), '11', 'es', 'Código INE'),
-- Córdoba
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '14'), 'cordoba', 'es', 'Nombre sin tilde'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '14'), 'cor', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '14'), '14', 'es', 'Código INE'),
-- Granada
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '18'), 'granada', 'es', 'Nombre completo'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '18'), 'gra', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '18'), '18', 'es', 'Código INE'),
-- Huelva
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '21'), 'huelva', 'es', 'Nombre completo'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '21'), 'hue', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '21'), '21', 'es', 'Código INE'),
-- Jaén
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '23'), 'jaen', 'es', 'Nombre sin tilde'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '23'), 'jae', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '23'), '23', 'es', 'Código INE'),
-- Málaga
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '29'), 'malaga', 'es', 'Nombre sin tilde'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '29'), 'mal', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '29'), 'mlg', 'es', 'Abreviación aeroportuaria'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '29'), '29', 'es', 'Código INE'),
-- Sevilla
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '41'), 'sevilla', 'es', 'Nombre completo'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '41'), 'sev', 'es', 'Abreviación'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '41'), 'svq', 'es', 'Código aeroportuario'),
((SELECT id_provincia FROM provincias WHERE codigo_provincia = '41'), '41', 'es', 'Código INE');

-- Alias para mercados turísticos
INSERT INTO mercados_alias (id_mercado, alias, lang, descripcion) VALUES
-- Alemanes
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'alemanes'), 'alemania', 'es', 'País de origen'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'alemanes'), 'germany', 'en', 'Nombre en inglés'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'alemanes'), 'de', 'es', 'Código ISO país'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'alemanes'), 'german', 'en', 'Gentilicio inglés'),
-- Británicos
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'britanicos'), 'reino unido', 'es', 'País completo'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'britanicos'), 'uk', 'en', 'Abreviación inglesa'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'britanicos'), 'gb', 'es', 'Código ISO país'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'britanicos'), 'british', 'en', 'Gentilicio inglés'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'britanicos'), 'ingleses', 'es', 'Gentilicio popular'),
-- Extranjeros
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'extranjeros'), 'internacional', 'es', 'Sinónimo'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'extranjeros'), 'foreign', 'en', 'Término inglés'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'extranjeros'), 'total extranjero', 'es', 'Descripción completa'),
-- Resto España
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'resto_espana'), 'resto españa', 'es', 'Con espacio'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'resto_espana'), 'resto_españa', 'es', 'Con tilde'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'resto_espana'), 'peninsular', 'es', 'Sinónimo geográfico'),
-- Andaluces
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'andaluces'), 'andalucia', 'es', 'Región sin tilde'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'andaluces'), 'andalucía', 'es', 'Región con tilde'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'andaluces'), 'regional', 'es', 'Tipo de mercado'),
-- Españoles
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'espanoles'), 'españa', 'es', 'País sin tilde'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'espanoles'), 'spain', 'en', 'Nombre inglés'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'espanoles'), 'nacional', 'es', 'Ámbito'),
((SELECT id_mercado FROM mercados WHERE codigo_mercado = 'espanoles'), 'es', 'es', 'Código ISO país');

-- Alias para tipos de turismo
INSERT INTO tipos_turismo_alias (id_tipo_turismo, alias, lang, descripcion) VALUES
-- Cultural
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'cultural'), 'cultura', 'es', 'Sinónimo'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'cultural'), 'patrimonio', 'es', 'Relacionado'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'cultural'), 'monuments', 'en', 'Término inglés'),
-- Ciudad
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'ciudad'), 'urbano', 'es', 'Sinónimo'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'ciudad'), 'city', 'en', 'Término inglés'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'ciudad'), 'metropolitan', 'en', 'Relacionado'),
-- Cruceros
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'cruceros'), 'crucero', 'es', 'Singular'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'cruceros'), 'cruise', 'en', 'Término inglés'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'cruceros'), 'maritime', 'en', 'Relacionado'),
-- Interior
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'interior'), 'rural', 'es', 'Sinónimo'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'interior'), 'inland', 'en', 'Término inglés'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'interior'), 'countryside', 'en', 'Relacionado'),
-- Litoral
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'litoral'), 'costa', 'es', 'Sinónimo'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'litoral'), 'playa', 'es', 'Relacionado'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'litoral'), 'beach', 'en', 'Término inglés'),
((SELECT id_tipo_turismo FROM tipos_turismo WHERE codigo_tipo = 'litoral'), 'coastal', 'en', 'Adjetivo inglés');

-- Alias para indicadores principales
INSERT INTO indicadores_alias (id_indicador, alias, lang, descripcion) VALUES
-- Viajeros
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'viajeros_hoteles_total'), 'viajeros', 'es', 'Forma corta'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'viajeros_hoteles_total'), 'travelers', 'en', 'Término inglés'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'viajeros_hoteles_total'), 'turistas hoteles', 'es', 'Sinónimo'),
-- Pernoctaciones
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'pernoctaciones_hoteles_total'), 'pernoctaciones', 'es', 'Forma corta'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'pernoctaciones_hoteles_total'), 'noches', 'es', 'Sinónimo popular'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'pernoctaciones_hoteles_total'), 'nights', 'en', 'Término inglés'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'pernoctaciones_hoteles_total'), 'overnight stays', 'en', 'Término técnico inglés'),
-- Ocupación
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'ocupacion_hotelera'), 'ocupacion', 'es', 'Forma corta'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'ocupacion_hotelera'), 'occupancy', 'en', 'Término inglés'),
-- Estancia
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'estancia_media'), 'estancia', 'es', 'Forma corta'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'estancia_media'), 'dias', 'es', 'Unidad'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'estancia_media'), 'length of stay', 'en', 'Término inglés'),
-- Gasto
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'gasto_medio_diario'), 'gasto', 'es', 'Forma corta'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'gasto_medio_diario'), 'spending', 'en', 'Término inglés'),
((SELECT id_indicador FROM indicadores WHERE codigo_indicador = 'gasto_medio_diario'), 'euros', 'es', 'Unidad');

-- =============================================================================
-- COMENTARIOS EN TABLAS
-- =============================================================================

COMMENT ON TABLE provincias IS 'Catálogo de provincias de Andalucía';
COMMENT ON TABLE mercados IS 'Catálogo de mercados turísticos por origen';
COMMENT ON TABLE tipos_turismo IS 'Catálogo de tipos de turismo';
COMMENT ON TABLE indicadores IS 'Catálogo de indicadores turísticos';
COMMENT ON TABLE datos_turismo IS 'Tabla principal con todos los datos turísticos mensuales';
COMMENT ON TABLE dim_fecha_mes IS 'Dimensión temporal con información de meses y períodos';
COMMENT ON TABLE mercados_alias IS 'Alias y sinónimos para búsqueda semántica de mercados';
COMMENT ON TABLE provincias_alias IS 'Alias y sinónimos para búsqueda semántica de provincias';
COMMENT ON TABLE indicadores_alias IS 'Alias y sinónimos para búsqueda semántica de indicadores';
COMMENT ON TABLE tipos_turismo_alias IS 'Alias y sinónimos para búsqueda semántica de tipos de turismo';

COMMENT ON COLUMN datos_turismo.variacion_interanual IS 'Variación porcentual respecto al mismo período del año anterior (decimal, ej: -0.03 = -3%)';
COMMENT ON COLUMN datos_turismo.archivo_origen IS 'Nombre del archivo Excel del cual se extrajo el dato';
COMMENT ON COLUMN datos_turismo.ambito_geografico IS 'Indica si el dato es provincial o de toda Andalucía';
COMMENT ON COLUMN datos_turismo.prov_key IS 'Clave generada para resolver problema de NULLs en UNIQUE constraint (provincia)';
COMMENT ON COLUMN datos_turismo.mercado_key IS 'Clave generada para resolver problema de NULLs en UNIQUE constraint (mercado)';
COMMENT ON COLUMN datos_turismo.tipo_key IS 'Clave generada para resolver problema de NULLs en UNIQUE constraint (tipo turismo)';
COMMENT ON COLUMN datos_turismo.date_id IS 'Clave temporal en formato YYYYMM (ej: 202501 = enero 2025)';

COMMENT ON VIEW v_kpis_wide IS 'Vista con KPIs principales pivoteados como columnas para análisis más fácil';

-- =============================================================================
-- VERIFICACIÓN DE INSTALACIÓN
-- =============================================================================

DO $$
DECLARE
    tabla_count INTEGER;
    vista_count INTEGER;
    funcion_count INTEGER;
BEGIN
    -- Contar tablas
    SELECT count(*) INTO tabla_count
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

    -- Contar vistas
    SELECT count(*) INTO vista_count
    FROM information_schema.views
    WHERE table_schema = 'public';

    -- Contar funciones
    SELECT count(*) INTO funcion_count
    FROM information_schema.routines
    WHERE routine_schema = 'public';

    RAISE NOTICE '✅ Instalación completada exitosamente:';
    RAISE NOTICE '  - Tablas creadas: %', tabla_count;
    RAISE NOTICE '  - Vistas creadas: %', vista_count;
    RAISE NOTICE '  - Funciones creadas: %', funcion_count;

    -- Verificar datos de catálogo
    RAISE NOTICE '📊 Datos de catálogo cargados:';
    RAISE NOTICE '  - Provincias: % registros', (SELECT count(*) FROM provincias);
    RAISE NOTICE '  - Mercados: % registros', (SELECT count(*) FROM mercados);
    RAISE NOTICE '  - Tipos turismo: % registros', (SELECT count(*) FROM tipos_turismo);
    RAISE NOTICE '  - Indicadores: % registros', (SELECT count(*) FROM indicadores);
    RAISE NOTICE '  - Fechas (2021-2025): % registros', (SELECT count(*) FROM dim_fecha_mes);
    RAISE NOTICE '  - Alias provincias: % registros', (SELECT count(*) FROM provincias_alias);
    RAISE NOTICE '  - Alias mercados: % registros', (SELECT count(*) FROM mercados_alias);

    RAISE NOTICE '🎯 Base de datos NEXUS lista para usar!';
END $$;