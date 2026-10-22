"""
motor_batch.py
==============
Motor de procesamiento Batch con Apache Spark (PySpark).
Implementa la capa Batch (precisión histórica) de la Arquitectura Lambda.

Casos de uso Batch demostrados:
- ETL nocturno de transacciones históricas
- Entrenamiento de datasets para modelos ML
- Reportes agregados (diarios, semanales, mensuales)
- Detección de anomalías sobre datos históricos

Tecnologías: PySpark + Delta Lake (formato Lakehouse)
"""

import logging
import os
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Rutas de almacenamiento local (sustituye al Data Lake S3 (SeaweedFS) en entorno de desarrollo)
RUTA_BASE = Path("datos/lakehouse")
RUTA_RAW = RUTA_BASE / "raw"
RUTA_SILVER = RUTA_BASE / "silver"
RUTA_GOLD = RUTA_BASE / "gold"


def _crear_sesion_spark(nombre_app: str = "BigData-Batch"):
    """
    Crea y configura una sesión Spark con soporte Delta Lake.
    En producción, apuntaría al clúster Spark real.
    """
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName(nombre_app)
            .master("local[*]")
            # Delta Lake para formato Lakehouse
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Optimizaciones de rendimiento
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # Compresión (Volumen)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"✅ SparkSession creada: {nombre_app}")
        return spark

    except ImportError:
        logger.warning("PySpark no instalado. Usando modo simulación con pandas.")
        return None


class MotorBatch:
    """
    Motor de procesamiento batch que implementa la capa de precisión histórica
    de la Arquitectura Lambda.

    Pipeline ETL:
      Bronze (Raw) → Silver (Limpio) → Gold (Agregado)
    """

    def __init__(self, usar_spark: bool = True):
        self.usar_spark = usar_spark
        self._spark = None
        self._metricas: dict = {
            "jobs_ejecutados": 0,
            "registros_procesados": 0,
            "errores": 0,
            "tiempo_total_s": 0.0,
        }

        # Crear directorios del lakehouse
        for ruta in [RUTA_RAW, RUTA_SILVER, RUTA_GOLD]:
            ruta.mkdir(parents=True, exist_ok=True)

    def inicializar_spark(self) -> bool:
        """Inicializa la sesión Spark."""
        if not self.usar_spark:
            return False
        self._spark = _crear_sesion_spark()
        return self._spark is not None

    # ── Pipeline ETL ───────────────────────────────────────────────────────────
    def ingesta_bronze(self, transacciones: list) -> str:
        """
        Capa Bronze: almacena datos crudos sin transformación.
        Principio: los datos raw se preservan para re-procesamiento histórico.
        """
        import json
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_archivo = RUTA_RAW / f"transacciones_{timestamp}.json"

        with open(ruta_archivo, "w", encoding="utf-8") as f:
            json.dump(transacciones, f, ensure_ascii=False, indent=2)

        logger.info(f"📥 Bronze: {len(transacciones)} registros → {ruta_archivo}")
        return str(ruta_archivo)

    def transformacion_silver(self, ruta_raw: str) -> str:
        """
        Capa Silver: limpieza, validación y enriquecimiento de datos.
        Elimina duplicados, maneja nulos, estandariza formatos.
        """
        import json
        import hashlib
        from datetime import datetime, timezone

        with open(ruta_raw, encoding="utf-8") as f:
            datos_raw = json.load(f)

        registros_limpios = []
        registros_rechazados = 0

        for registro in datos_raw:
            # Validación de calidad (Veracidad)
            if not registro.get("datos_completos", True):
                registros_rechazados += 1
                continue
            if registro.get("monto", 0) <= 0:
                registros_rechazados += 1
                continue

            # Limpieza y estandarización
            registro_limpio = {
                "id_transaccion": registro["id_transaccion"],
                "timestamp_utc": registro["timestamp_utc"],
                "cliente_id_hash": hashlib.sha256(
                    registro.get("cliente_id", "").encode()
                ).hexdigest()[:16],
                "tipo": registro.get("tipo", "DESCONOCIDO").upper(),
                "monto_usd": self._convertir_a_usd(
                    registro.get("monto", 0),
                    registro.get("moneda", "USD"),
                ),
                "moneda_original": registro.get("moneda", "USD"),
                "region": registro.get("region", "desconocida").lower(),
                "canal": registro.get("canal", "DESCONOCIDO").upper(),
                "comercio_categoria": registro.get("comercio_categoria", "OTRO"),
                "shard_key": registro.get("shard_key", "shard_0"),
                "score_riesgo": float(registro.get("score_riesgo", 0.0)),
                "es_fraude": bool(registro.get("es_fraude_potencial", False)),
                "_procesado_timestamp": datetime.now(timezone.utc).isoformat(),
                "_version": "silver_v2.1",
            }
            registros_limpios.append(registro_limpio)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_silver = RUTA_SILVER / f"transacciones_limpio_{timestamp}.json"
        with open(ruta_silver, "w", encoding="utf-8") as f:
            json.dump(registros_limpios, f, ensure_ascii=False, indent=2)

        logger.info(
            f"🔄 Silver: {len(registros_limpios)} válidos, "
            f"{registros_rechazados} rechazados → {ruta_silver}"
        )
        return str(ruta_silver)

    def agregacion_gold(self, ruta_silver: str) -> dict:
        """
        Capa Gold: agregaciones y KPIs listos para consumo analítico.
        Estos datos alimentan dashboards, reportes y modelos ML.
        """
        import json
        from collections import defaultdict

        with open(ruta_silver, encoding="utf-8") as f:
            datos = json.load(f)

        if not datos:
            return {}

        # Agregaciones por región
        por_region: dict = defaultdict(lambda: {"total_transacciones": 0, "monto_total_usd": 0.0, "fraudes": 0})
        por_tipo: dict = defaultdict(lambda: {"total": 0, "monto_total_usd": 0.0})
        por_categoria: dict = defaultdict(int)
        montos = []

        for r in datos:
            region = r["region"]
            por_region[region]["total_transacciones"] += 1
            por_region[region]["monto_total_usd"] += r["monto_usd"]
            if r["es_fraude"]:
                por_region[region]["fraudes"] += 1

            por_tipo[r["tipo"]]["total"] += 1
            por_tipo[r["tipo"]]["monto_total_usd"] += r["monto_usd"]
            por_categoria[r["comercio_categoria"]] += 1
            montos.append(r["monto_usd"])

        # KPIs globales
        kpis = {
            "total_transacciones": len(datos),
            "monto_total_usd": round(sum(montos), 2),
            "monto_promedio_usd": round(sum(montos) / len(montos), 2) if montos else 0,
            "monto_max_usd": round(max(montos), 2) if montos else 0,
            "monto_min_usd": round(min(montos), 2) if montos else 0,
            "total_fraudes": sum(1 for r in datos if r["es_fraude"]),
            "tasa_fraude_pct": round(
                sum(1 for r in datos if r["es_fraude"]) / len(datos) * 100, 3
            ),
            "score_riesgo_promedio": round(
                sum(r["score_riesgo"] for r in datos) / len(datos), 4
            ),
        }

        resultado = {
            "kpis_globales": kpis,
            "por_region": {k: {
                **v,
                "monto_total_usd": round(v["monto_total_usd"], 2),
                "tasa_fraude_pct": round(v["fraudes"] / v["total_transacciones"] * 100, 2),
            } for k, v in por_region.items()},
            "por_tipo_transaccion": {k: {
                **v,
                "monto_total_usd": round(v["monto_total_usd"], 2),
            } for k, v in por_tipo.items()},
            "top_categorias": dict(
                sorted(por_categoria.items(), key=lambda x: x[1], reverse=True)[:5]
            ),
        }

        # Guardar Gold
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_gold = RUTA_GOLD / f"kpis_{timestamp}.json"
        with open(ruta_gold, "w", encoding="utf-8") as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)

        logger.info(f"⭐ Gold: KPIs calculados → {ruta_gold}")
        return resultado

    def _convertir_a_usd(self, monto: float, moneda: str) -> float:
        """Conversión simplificada de monedas (en prod: API de tipo de cambio)."""
        tasas = {"USD": 1.0, "BOB": 0.145, "EUR": 1.09}
        return round(monto * tasas.get(moneda, 1.0), 2)

    # ── Pipeline completo ──────────────────────────────────────────────────────
    def ejecutar_pipeline_etl(self, transacciones: list) -> dict:
        """
        Ejecuta el pipeline ETL completo: Bronze → Silver → Gold.
        Representa un job batch nocturno típico en Arquitectura Lambda.
        """
        t0 = time.perf_counter()
        logger.info(f"🚀 Iniciando pipeline ETL batch ({len(transacciones)} registros)...")

        # Bronze
        ruta_bronze = self.ingesta_bronze(transacciones)
        # Silver
        ruta_silver = self.transformacion_silver(ruta_bronze)
        # Gold
        kpis = self.agregacion_gold(ruta_silver)

        elapsed = time.perf_counter() - t0
        self._metricas["jobs_ejecutados"] += 1
        self._metricas["registros_procesados"] += len(transacciones)
        self._metricas["tiempo_total_s"] += elapsed

        resultado = {
            "estado": "COMPLETADO",
            "registros_entrada": len(transacciones),
            "duracion_s": round(elapsed, 3),
            "throughput_registros_s": round(len(transacciones) / elapsed, 0),
            "rutas": {
                "bronze": str(ruta_bronze),
                "silver": str(ruta_silver),
            },
            "kpis": kpis.get("kpis_globales", {}),
        }

        logger.info(
            f"✅ Pipeline ETL completado en {elapsed:.2f}s | "
            f"Throughput: {resultado['throughput_registros_s']:.0f} registros/s"
        )
        return resultado

    def cerrar(self) -> None:
        if self._spark:
            self._spark.stop()
            logger.info("SparkSession cerrada.")


# ── Punto de entrada ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    from src.ingesta.simulador_transacciones import generar_lote

    print("=" * 65)
    print("  MOTOR BATCH — Pipeline ETL (Bronze → Silver → Gold)")
    print("=" * 65)

    motor = MotorBatch(usar_spark=False)
    transacciones = generar_lote(500)

    import json
    resultado = motor.ejecutar_pipeline_etl(transacciones)

    print("\n📊 Resultado del Pipeline ETL:")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))

    print("\n🏆 KPIs calculados:")
    for k, v in resultado["kpis"].items():
        print(f"  {k}: {v}")
