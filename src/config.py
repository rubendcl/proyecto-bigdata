"""
config.py
=========
Configuración centralizada para el proyecto de Arquitecturas Big Data.
Basado en las mejores prácticas de diseño: desacoplamiento de componentes,
infraestructura como código y configuración mediante variables de entorno.
"""

import os
from dataclasses import dataclass, field
from typing import List


# ─────────────────────────────────────────────
#  KAFKA — Capa de Velocidad (Streaming)
# ─────────────────────────────────────────────
@dataclass
class ConfigKafka:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topico_transacciones: str = "transacciones-financieras"
    topico_alertas: str = "alertas-fraude"
    topico_metricas: str = "metricas-sistema"
    grupo_consumidores: str = "grupo-procesamiento-bd"
    factor_replicacion: int = 2          # N+1 recomendado por componente crítico
    num_particiones: int = 6             # Particionamiento funcional
    timeout_ms: int = 30_000


# ─────────────────────────────────────────────
#  POSTGRESQL — Data Warehouse
# ─────────────────────────────────────────────
@dataclass
class ConfigPostgres:
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    puerto: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    base_datos: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "bigdata_dw"))
    usuario: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "bigdata"))
    contrasena: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "bigdata2026"))
    pool_min: int = 2
    pool_max: int = 10

    @property
    def cadena_conexion(self) -> str:
        return (
            f"postgresql://{self.usuario}:{self.contrasena}"
            f"@{self.host}:{self.puerto}/{self.base_datos}"
        )


# ─────────────────────────────────────────────
#  SEAWEEDFS — Data Lake (Object Storage S3-compatible)
#  Reemplaza a MinIO; cualquier cliente S3 (boto3, s3fs, Spark s3a) funciona.
# ─────────────────────────────────────────────
@dataclass
class ConfigS3:
    endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", "localhost:8333"))
    access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", "lakeadmin"))
    secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", "lakeadmin123"))
    bucket_raw: str = "zona-raw"           # Datos crudos (Bronze)
    bucket_procesado: str = "zona-silver"  # Datos limpios (Silver)
    bucket_curado: str = "zona-gold"       # Datos listos para negocio (Gold)
    bucket_lakehouse: str = "lakehouse"    # Formato Delta / Iceberg
    usar_ssl: bool = False
    region: str = "us-east-1"

    @property
    def url(self) -> str:
        return f"{'https' if self.usar_ssl else 'http'}://{self.endpoint}"


# ─────────────────────────────────────────────
#  SPARK — Motor de Procesamiento
# ─────────────────────────────────────────────
@dataclass
class ConfigSpark:
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    nombre_app: str = "BigData-ArquitecturasRobustas-2026"
    memoria_driver: str = "2g"
    memoria_executor: str = "2g"
    num_executors: int = 2
    delta_version: str = "2.4.0"


# ─────────────────────────────────────────────
#  RECUPERACIÓN ANTE DESASTRES — DRP / BCP
# ─────────────────────────────────────────────
@dataclass
class ConfigRecuperacion:
    # RTO: tiempo máximo tolerable para restaurar el servicio
    rto_objetivo_minutos: int = 15
    # RPO: máxima cantidad de datos (en tiempo) que se puede perder
    rpo_objetivo_minutos: int = 5
    intervalo_backup_segundos: int = 300   # Cada 5 minutos
    sitio_primario: str = "region-a-activa"
    sitio_standby: str = "region-b-standby"
    sitio_cold: str = "region-c-backup-frio"
    max_reintentos: int = 3


# ─────────────────────────────────────────────
#  ALTA DISPONIBILIDAD — Resiliencia
# ─────────────────────────────────────────────
@dataclass
class ConfigResiliencia:
    # Circuit Breaker
    umbral_fallos_cb: int = 5          # Fallos antes de abrir el circuito
    timeout_circuito_abierto_s: int = 30
    ventana_fallos_s: int = 60

    # Particionamiento horizontal (Sharding)
    num_shards: int = 3
    estrategia_shard: str = "hash"     # hash | range | geografica

    # Replicación
    tipo_replicacion: str = "asincrona"  # sincrona | asincrona | multi-lider
    factor_replicacion: int = 2

    # Disponibilidad objetivo (4 nueves)
    disponibilidad_objetivo: float = 99.99


# ─────────────────────────────────────────────
#  CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────
class Configuracion:
    """Punto único de acceso a toda la configuración del sistema."""

    kafka = ConfigKafka()
    postgres = ConfigPostgres()
    s3 = ConfigS3()
    spark = ConfigSpark()
    recuperacion = ConfigRecuperacion()
    resiliencia = ConfigResiliencia()

    # Regiones disponibles para particionamiento geográfico
    REGIONES: List[str] = ["norte", "sur", "este", "oeste", "centro"]

    # Zonas de disponibilidad (Multi-AZ)
    ZONAS_DISPONIBILIDAD: List[str] = ["az-1", "az-2", "az-3"]

    # Nivel de log
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def desde_env(cls) -> "Configuracion":
        """Carga configuración desde variables de entorno (12-factor app)."""
        return cls()
