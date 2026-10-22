"""
pipeline_arquitectura.py
========================
DAG de Apache Airflow para la orquestación del pipeline de Big Data.

Implementa la Arquitectura Lambda con:
  - Capa Batch (ingesta y ETL nocturnos)
  - Capa de Velocidad (validación de streaming)
  - Capa de Servicio (actualización de datos Gold)

La orquestación con Airflow garantiza:
  - Programación y dependencias entre tareas
  - Reintentos automáticos (tolerancia a fallos)
  - Alertas por correo ante fallos
  - Linaje de datos (trazabilidad)
  - SLA monitoring

Documentación: https://airflow.apache.org/docs/
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Argumentos por defecto ─────────────────────────────────────────────────────
ARGS_DEFAULT = {
    "owner": "equipo-bigdata",
    "depends_on_past": False,
    "email": ["alertas-bigdata@empresa.bo"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

# ── Constantes ─────────────────────────────────────────────────────────────────
LOTE_TRANSACCIONES = 10_000
UMBRAL_TASA_FRAUDE = 0.05  # 5% → disparar alerta


# ── Funciones de las tareas ────────────────────────────────────────────────────

def verificar_disponibilidad_kafka(**context) -> bool:
    """
    T1: Verifica que Kafka esté disponible antes de iniciar el pipeline.
    Implementa el principio de eliminación de puntos únicos de falla.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers="kafka:29092", client_id="health-check")
        topics = admin.list_topics()
        admin.close()
        logger.info(f"✅ Kafka disponible. Tópicos: {topics}")
        context["ti"].xcom_push(key="kafka_disponible", value=True)
        return True
    except Exception as e:
        logger.error(f"❌ Kafka no disponible: {e}")
        context["ti"].xcom_push(key="kafka_disponible", value=False)
        return False


def generar_datos_simulados(**context) -> dict:
    """T2: Genera el lote de transacciones para el ciclo batch."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../")
    from src.ingesta.simulador_transacciones import generar_lote, guardar_muestra_json

    run_id = context["run_id"]
    n = context["params"].get("n_transacciones", LOTE_TRANSACCIONES)

    transacciones = generar_lote(n)
    ruta = f"/tmp/batch_{run_id.replace(':', '_')}.json"

    import json
    with open(ruta, "w") as f:
        json.dump(transacciones, f)

    context["ti"].xcom_push(key="ruta_batch", value=ruta)
    context["ti"].xcom_push(key="n_registros", value=len(transacciones))
    return {"ruta": ruta, "n_registros": len(transacciones)}


def ejecutar_etl_bronze_silver(**context) -> str:
    """T3: Ejecuta la transformación Bronze → Silver (limpieza y validación)."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../")
    from src.procesamiento.motor_batch import MotorBatch
    import json

    ruta_batch = context["ti"].xcom_pull(task_ids="generar_datos", key="ruta_batch")
    with open(ruta_batch) as f:
        transacciones = json.load(f)

    motor = MotorBatch(usar_spark=False)
    ruta_bronze = motor.ingesta_bronze(transacciones)
    ruta_silver = motor.transformacion_silver(ruta_bronze)

    context["ti"].xcom_push(key="ruta_silver", value=ruta_silver)
    return ruta_silver


def ejecutar_etl_silver_gold(**context) -> dict:
    """T4: Ejecuta la agregación Silver → Gold (KPIs y métricas de negocio)."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../")
    from src.procesamiento.motor_batch import MotorBatch

    ruta_silver = context["ti"].xcom_pull(task_ids="etl_bronze_silver", key="ruta_silver")
    motor = MotorBatch(usar_spark=False)
    kpis = motor.agregacion_gold(ruta_silver)

    context["ti"].xcom_push(key="kpis", value=kpis)
    context["ti"].xcom_push(
        key="tasa_fraude",
        value=kpis.get("kpis_globales", {}).get("tasa_fraude_pct", 0),
    )
    return kpis


def evaluar_calidad_datos(**context) -> str:
    """
    T5 (Branch): Evalúa la calidad de los datos y decide el flujo.
    Si la tasa de fraude es alta → alerta. Si no → actualización normal.
    """
    tasa_fraude = context["ti"].xcom_pull(task_ids="etl_silver_gold", key="tasa_fraude")
    if tasa_fraude and tasa_fraude > UMBRAL_TASA_FRAUDE * 100:
        return "notificar_alerta_fraude"
    return "actualizar_lakehouse"


def notificar_alerta_fraude(**context):
    """T6a: Notifica una tasa de fraude inusualmente alta."""
    import logging
    logger = logging.getLogger(__name__)
    tasa = context["ti"].xcom_pull(task_ids="etl_silver_gold", key="tasa_fraude")
    logger.critical(
        f"🚨 ALERTA: Tasa de fraude en {tasa:.2f}% "
        f"(umbral: {UMBRAL_TASA_FRAUDE*100:.0f}%). "
        f"Notificando al equipo de seguridad..."
    )
    # En prod: enviar a PagerDuty / Slack / Email


def actualizar_lakehouse(**context):
    """T6b: Actualiza la capa Gold del Lakehouse con los datos procesados."""
    import logging
    logger = logging.getLogger(__name__)
    kpis = context["ti"].xcom_pull(task_ids="etl_silver_gold", key="kpis")
    globales = kpis.get("kpis_globales", {})
    logger.info(
        f"✅ Lakehouse Gold actualizado. "
        f"Total: {globales.get('total_transacciones', 0):,} registros | "
        f"Fraude: {globales.get('tasa_fraude_pct', 0):.2f}%"
    )
    # En prod: escribir a Delta Lake / Iceberg en SeaweedFS (S3)


def verificar_backup_drp(**context):
    """T7: Verifica que el backup del sitio de respaldo esté actualizado (RPO)."""
    import logging
    import time
    logger = logging.getLogger(__name__)

    tiempo_ultimo_backup = time.time()  # Simulado
    rpo_objetivo_s = 5 * 60  # 5 minutos

    tiempo_sin_backup = time.time() - tiempo_ultimo_backup
    if tiempo_sin_backup > rpo_objetivo_s:
        raise Exception(
            f"❌ RPO EXCEDIDO: Último backup hace {tiempo_sin_backup/60:.1f} min "
            f"(objetivo: {rpo_objetivo_s/60:.0f} min)"
        )

    logger.info(f"✅ RPO OK. Último backup hace {tiempo_sin_backup:.0f}s.")


def registrar_linaje(**context):
    """T8: Registra el linaje de datos para auditoría y cumplimiento."""
    import logging
    logger = logging.getLogger(__name__)

    n_registros = context["ti"].xcom_pull(task_ids="generar_datos", key="n_registros")
    ruta_silver = context["ti"].xcom_pull(task_ids="etl_bronze_silver", key="ruta_silver")

    linaje = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "fecha_ejecucion": context["ds"],
        "registros_procesados": n_registros,
        "origen": "simulador-transacciones-financieras",
        "destino_silver": ruta_silver,
        "destino_gold": "lakehouse/gold/",
        "transformaciones": ["limpieza", "deduplicacion", "anonimizacion", "conversion_moneda"],
        "responsable": "equipo-bigdata",
        "cumple_gdpr": True,
    }
    logger.info(f"📋 Linaje registrado: {linaje}")
    # En prod: escribir a Apache Atlas / OpenLineage / DataHub


# ── Definición del DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="pipeline_bigdata_arquitecturas",
    description="Pipeline ETL completo — Arquitectura Lambda con DRP y observabilidad",
    default_args=ARGS_DEFAULT,
    schedule_interval="0 2 * * *",      # Cada día a las 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bigdata", "etl", "lambda", "drp"],
    doc_md="""
## Pipeline Big Data — Arquitectura Lambda

Este DAG implementa el pipeline completo de datos siguiendo la
**Arquitectura Lambda** con tres capas:

### Flujo de ejecución
```
inicio → verificar_kafka → generar_datos → etl_bronze_silver
       → etl_silver_gold → evaluar_calidad → [actualizar_lakehouse | alerta_fraude]
       → verificar_drp → registrar_linaje → fin
```

### SLA
- **RTO objetivo**: 15 minutos
- **RPO objetivo**: 5 minutos
- **Disponibilidad**: 99.99% ("4 nueves")

### Contacto
- **Equipo**: Maestría Big Data 2026
- **Docente**: MSc. Ing. Marcelo Fernando Condori Mendoza
    """,
    params={
        "n_transacciones": LOTE_TRANSACCIONES,
        "umbral_fraude": UMBRAL_TASA_FRAUDE,
    },
) as dag:

    # ── Nodos del DAG ──────────────────────────────────────────────────────────
    inicio = EmptyOperator(task_id="inicio")
    fin = EmptyOperator(task_id="fin", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    verificar_kafka = PythonOperator(
        task_id="verificar_kafka",
        python_callable=verificar_disponibilidad_kafka,
    )

    generar_datos = PythonOperator(
        task_id="generar_datos",
        python_callable=generar_datos_simulados,
    )

    etl_bronze_silver = PythonOperator(
        task_id="etl_bronze_silver",
        python_callable=ejecutar_etl_bronze_silver,
    )

    etl_silver_gold = PythonOperator(
        task_id="etl_silver_gold",
        python_callable=ejecutar_etl_silver_gold,
    )

    evaluar_calidad = BranchPythonOperator(
        task_id="evaluar_calidad",
        python_callable=evaluar_calidad_datos,
    )

    tarea_alerta = PythonOperator(
        task_id="notificar_alerta_fraude",
        python_callable=notificar_alerta_fraude,
    )

    tarea_lakehouse = PythonOperator(
        task_id="actualizar_lakehouse",
        python_callable=actualizar_lakehouse,
    )

    verificar_drp = PythonOperator(
        task_id="verificar_backup_drp",
        python_callable=verificar_backup_drp,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    registrar_linaje_tarea = PythonOperator(
        task_id="registrar_linaje",
        python_callable=registrar_linaje,
    )

    # ── Dependencias ───────────────────────────────────────────────────────────
    (
        inicio
        >> verificar_kafka
        >> generar_datos
        >> etl_bronze_silver
        >> etl_silver_gold
        >> evaluar_calidad
        >> [tarea_alerta, tarea_lakehouse]
        >> verificar_drp
        >> registrar_linaje_tarea
        >> fin
    )
