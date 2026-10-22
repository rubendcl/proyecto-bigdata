-- ═══════════════════════════════════════════════════════════════════════════
-- init_warehouse.sql
-- Inicialización del Data Warehouse PostgreSQL
-- Arquitectura: esquema de estrella (Star Schema) para analítica
-- ═══════════════════════════════════════════════════════════════════════════

-- Extensiones útiles
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ── Esquemas ──────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;    -- Zona de aterrizaje temporal
CREATE SCHEMA IF NOT EXISTS gold;       -- Datos curados para analítica
CREATE SCHEMA IF NOT EXISTS control;    -- Metadatos de linaje y calidad

-- ══════════════════════════════════════════════════════════════════════════════
-- TABLAS DE DIMENSIONES (Star Schema)
-- ══════════════════════════════════════════════════════════════════════════════

-- Dimensión tiempo
CREATE TABLE IF NOT EXISTS gold.dim_tiempo (
    id_tiempo       SERIAL PRIMARY KEY,
    fecha           DATE NOT NULL UNIQUE,
    anio            SMALLINT NOT NULL,
    mes             SMALLINT NOT NULL,
    dia             SMALLINT NOT NULL,
    dia_semana      SMALLINT NOT NULL,  -- 1=Lunes, 7=Domingo
    nombre_mes      VARCHAR(20),
    es_fin_semana   BOOLEAN,
    trimestre       SMALLINT
);

-- Dimensión cliente (anonimizado - cumplimiento de privacidad)
CREATE TABLE IF NOT EXISTS gold.dim_cliente (
    id_cliente      SERIAL PRIMARY KEY,
    cliente_hash    VARCHAR(16) NOT NULL UNIQUE,   -- SHA-256 truncado (GDPR)
    region          VARCHAR(20),
    banco_emisor    VARCHAR(50),
    shard_key       VARCHAR(20),
    fecha_primera_tx DATE,
    fecha_ultima_tx  DATE
);

-- Dimensión comercio
CREATE TABLE IF NOT EXISTS gold.dim_comercio (
    id_comercio     SERIAL PRIMARY KEY,
    comercio_codigo VARCHAR(20) NOT NULL UNIQUE,
    nombre          VARCHAR(100),
    categoria       VARCHAR(50),
    region          VARCHAR(20)
);

-- Dimensión canal
CREATE TABLE IF NOT EXISTS gold.dim_canal (
    id_canal        SERIAL PRIMARY KEY,
    nombre          VARCHAR(30) NOT NULL UNIQUE,
    tipo            VARCHAR(20)  -- DIGITAL, FISICO, MIXTO
);

-- ══════════════════════════════════════════════════════════════════════════════
-- TABLA DE HECHOS (Fact Table)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold.fact_transacciones (
    id_hecho            BIGSERIAL PRIMARY KEY,
    -- Claves foráneas (Star Schema)
    id_tiempo           INTEGER REFERENCES gold.dim_tiempo(id_tiempo),
    id_cliente          INTEGER REFERENCES gold.dim_cliente(id_cliente),
    id_comercio         INTEGER REFERENCES gold.dim_comercio(id_comercio),
    id_canal            INTEGER REFERENCES gold.dim_canal(id_canal),
    -- Identificadores
    id_transaccion      UUID NOT NULL UNIQUE,
    timestamp_utc       TIMESTAMPTZ NOT NULL,
    -- Métricas (measures)
    monto_usd           NUMERIC(18, 4) NOT NULL,
    monto_original      NUMERIC(18, 4),
    moneda_original     CHAR(3),
    -- Indicadores de calidad y riesgo (Veracidad)
    es_fraude           BOOLEAN DEFAULT FALSE,
    score_riesgo        NUMERIC(5, 4),
    -- Particionamiento
    shard_key           VARCHAR(20),
    region              VARCHAR(20),
    -- Metadatos de carga
    fecha_carga         TIMESTAMPTZ DEFAULT NOW(),
    version_etl         VARCHAR(10) DEFAULT '2.1'
) PARTITION BY RANGE (timestamp_utc);   -- Particionamiento temporal

-- Particiones por año (particionamiento horizontal automático)
CREATE TABLE IF NOT EXISTS gold.fact_transacciones_2025
    PARTITION OF gold.fact_transacciones
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS gold.fact_transacciones_2026
    PARTITION OF gold.fact_transacciones
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- ── Índices para rendimiento de consultas ─────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_timestamp ON gold.fact_transacciones (timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_fact_cliente ON gold.fact_transacciones (id_cliente);
CREATE INDEX IF NOT EXISTS idx_fact_fraude ON gold.fact_transacciones (es_fraude) WHERE es_fraude = TRUE;
CREATE INDEX IF NOT EXISTS idx_fact_shard ON gold.fact_transacciones (shard_key);
CREATE INDEX IF NOT EXISTS idx_fact_region ON gold.fact_transacciones (region);

-- ══════════════════════════════════════════════════════════════════════════════
-- ZONA STAGING — Aterrizaje temporal de datos del pipeline Batch
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS staging.transacciones_raw (
    id              BIGSERIAL PRIMARY KEY,
    payload         JSONB NOT NULL,               -- Dato completo sin procesar
    fuente          VARCHAR(50),
    timestamp_carga TIMESTAMPTZ DEFAULT NOW(),
    procesado       BOOLEAN DEFAULT FALSE,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_procesado ON staging.transacciones_raw (procesado);

-- ══════════════════════════════════════════════════════════════════════════════
-- CONTROL DE LINAJE Y CALIDAD
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS control.linaje_pipeline (
    id              SERIAL PRIMARY KEY,
    dag_run_id      VARCHAR(200),
    fecha_ejecucion DATE NOT NULL,
    registros_entrada   INTEGER,
    registros_validos   INTEGER,
    registros_rechazados INTEGER,
    tasa_fraude_pct NUMERIC(6, 3),
    duracion_s      NUMERIC(10, 2),
    estado          VARCHAR(20),    -- COMPLETADO, FALLIDO, PARCIAL
    timestamp_inicio TIMESTAMPTZ,
    timestamp_fin   TIMESTAMPTZ,
    observaciones   TEXT
);

CREATE TABLE IF NOT EXISTS control.alertas_calidad (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ DEFAULT NOW(),
    tipo_alerta     VARCHAR(50),
    severidad       VARCHAR(20),
    mensaje         TEXT,
    resuelto        BOOLEAN DEFAULT FALSE,
    resuelto_en     TIMESTAMPTZ
);

-- ══════════════════════════════════════════════════════════════════════════════
-- VISTAS ANALÍTICAS (capa de servicio)
-- ══════════════════════════════════════════════════════════════════════════════

-- Vista: Resumen diario de transacciones
CREATE OR REPLACE VIEW gold.v_resumen_diario AS
SELECT
    dt.fecha,
    dt.anio,
    dt.mes,
    dc.region,
    dca.nombre AS canal,
    COUNT(*) AS total_transacciones,
    ROUND(SUM(ft.monto_usd), 2) AS monto_total_usd,
    ROUND(AVG(ft.monto_usd), 2) AS monto_promedio_usd,
    SUM(CASE WHEN ft.es_fraude THEN 1 ELSE 0 END) AS total_fraudes,
    ROUND(
        SUM(CASE WHEN ft.es_fraude THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 3
    ) AS tasa_fraude_pct
FROM gold.fact_transacciones ft
JOIN gold.dim_tiempo dt ON ft.id_tiempo = dt.id_tiempo
JOIN gold.dim_cliente dc ON ft.id_cliente = dc.id_cliente
JOIN gold.dim_canal dca ON ft.id_canal = dc.id_cliente
GROUP BY dt.fecha, dt.anio, dt.mes, dc.region, dca.nombre;

-- Vista: Top 10 clientes por volumen (anonimizados)
CREATE OR REPLACE VIEW gold.v_top_clientes AS
SELECT
    dc.cliente_hash,
    dc.region,
    COUNT(*) AS total_transacciones,
    ROUND(SUM(ft.monto_usd), 2) AS monto_total_usd,
    ROUND(AVG(ft.score_riesgo), 4) AS score_riesgo_promedio
FROM gold.fact_transacciones ft
JOIN gold.dim_cliente dc ON ft.id_cliente = dc.id_cliente
GROUP BY dc.cliente_hash, dc.region
ORDER BY monto_total_usd DESC
LIMIT 10;

-- ── Mensaje de confirmación ────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE '✅ Data Warehouse inicializado correctamente.';
    RAISE NOTICE '   Esquemas: staging, gold, control';
    RAISE NOTICE '   Tablas: fact_transacciones (particionada), dim_*, staging.transacciones_raw';
    RAISE NOTICE '   Vistas: v_resumen_diario, v_top_clientes';
END $$;
