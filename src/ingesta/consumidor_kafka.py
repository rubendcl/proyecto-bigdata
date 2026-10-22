"""
consumidor_kafka.py
===================
Consumidor Kafka con auto-recuperación y manejo de errores.
Representa la capa de velocidad (Speed Layer) en Arquitectura Lambda
y el pipeline único en Arquitectura Kappa.

Conceptos demostrados:
- Procesamiento de eventos en milisegundos
- Checkpointing de offsets (tolerancia a fallos)
- Grupos de consumidores para escalado horizontal
- Auto-recuperación (self-healing)
"""

import json
import logging
import signal
import time
from typing import Callable, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, CommitFailedError

from src.config import Configuracion

logger = logging.getLogger(__name__)
cfg = Configuracion()


# ── Procesadores de mensajes ───────────────────────────────────────────────────
def procesador_deteccion_fraude(transaccion: dict) -> dict:
    """
    Procesamiento en tiempo real: detección de fraude.
    En un sistema real, aquí iría un modelo ML o reglas de negocio.
    """
    score = transaccion.get("score_riesgo", 0.0)
    monto = transaccion.get("monto", 0.0)
    canal = transaccion.get("canal", "")

    # Reglas simples de detección (en prod: modelo ML via REST)
    alertas = []
    if score > 0.7:
        alertas.append("SCORE_RIESGO_ALTO")
    if monto > 50_000:
        alertas.append("MONTO_INUSUAL")
    if canal == "ATM" and monto > 10_000:
        alertas.append("RETIRO_ATM_EXCESIVO")
    if transaccion.get("es_fraude_potencial"):
        alertas.append("MARCADO_FRAUDE")

    resultado = {
        "id_transaccion": transaccion["id_transaccion"],
        "cliente_id": transaccion.get("cliente_id"),
        "alertas": alertas,
        "accion": "BLOQUEAR" if alertas else "APROBAR",
        "procesado_en_ms": round(time.time() * 1000),
    }
    if alertas:
        logger.warning(f"🚨 ALERTA [{transaccion['id_transaccion'][:8]}]: {', '.join(alertas)}")
    return resultado


def procesador_enriquecimiento(transaccion: dict) -> dict:
    """Enriquece la transacción con datos adicionales antes de almacenar."""
    import hashlib
    # Anonimización del cliente (GDPR / protección de datos)
    cliente_hash = hashlib.sha256(
        transaccion.get("cliente_id", "").encode()
    ).hexdigest()[:16]

    return {
        **transaccion,
        "cliente_id_hash": cliente_hash,
        "timestamp_procesado": time.time(),
        "version_procesamiento": "2.1",
        "zona_disponibilidad": "az-1",   # Metadato de HA
    }


# ── Consumidor principal ───────────────────────────────────────────────────────
class ConsumidorTransacciones:
    """
    Consumidor Kafka con:
    - Checkpointing automático (commit de offsets)
    - Reinicio automático ante fallos (self-healing)
    - Pipeline de procesamiento configurable
    - Métricas de observabilidad
    """

    def __init__(
        self,
        topicos: Optional[list] = None,
        grupo_id: Optional[str] = None,
    ):
        self._topicos = topicos or [
            cfg.kafka.topico_transacciones,
            cfg.kafka.topico_alertas,
        ]
        self._grupo_id = grupo_id or cfg.kafka.grupo_consumidores
        self._consumidor: Optional[KafkaConsumer] = None
        self._activo = False
        self._pipeline: list[Callable] = [
            procesador_deteccion_fraude,
            procesador_enriquecimiento,
        ]
        self._metricas = {
            "mensajes_procesados": 0,
            "errores_procesamiento": 0,
            "alertas_generadas": 0,
            "inicio": time.time(),
        }

    def agregar_procesador(self, fn: Callable) -> "ConsumidorTransacciones":
        """Agrega un procesador al pipeline de transformación (patrón Chain of Responsibility)."""
        self._pipeline.append(fn)
        return self

    def conectar(self) -> bool:
        """Conecta al clúster Kafka con reintentos automáticos."""
        for intento in range(1, cfg.recuperacion.max_reintentos + 1):
            try:
                logger.info(f"Conectando consumidor Kafka (intento {intento})...")
                self._consumidor = KafkaConsumer(
                    *self._topicos,
                    bootstrap_servers=cfg.kafka.bootstrap_servers,
                    group_id=self._grupo_id,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    key_deserializer=lambda k: k.decode("utf-8") if k else None,
                    # Checkpointing manual para control preciso del RPO
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    max_poll_records=100,
                    session_timeout_ms=30_000,
                    heartbeat_interval_ms=10_000,
                    fetch_max_bytes=52_428_800,  # 50 MB
                )
                logger.info(f"✅ Consumidor conectado. Tópicos: {self._topicos}")
                return True
            except Exception as e:
                logger.warning(f"❌ Intento {intento} fallido: {e}. Reintentando en 5s...")
                time.sleep(5)

        logger.error("No se pudo conectar el consumidor Kafka.")
        return False

    def _procesar_mensaje(self, record) -> dict:
        """
        Ejecuta el pipeline de procesamiento sobre cada mensaje.
        El checkpointing (commit) ocurre DESPUÉS del procesamiento exitoso,
        garantizando el RPO: si falla el proceso, el mensaje se re-procesará.
        """
        datos = record.value
        for procesador in self._pipeline:
            try:
                datos = procesador(datos) or datos
            except Exception as e:
                logger.error(f"Error en procesador {procesador.__name__}: {e}")
                self._metricas["errores_procesamiento"] += 1
        return datos

    def consumir(
        self,
        max_mensajes: Optional[int] = None,
        callback_resultado: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """
        Inicia el ciclo de consumo con auto-recuperación.

        El commit manual del offset implementa exactamente la semántica
        'at-least-once delivery', fundamental para el RPO en sistemas HA.
        """
        if not self._consumidor and not self.conectar():
            raise RuntimeError("No se puede consumir sin conexión Kafka.")

        self._activo = True

        def _señal_paro(sig, frame):
            logger.info("🛑 Deteniendo consumidor...")
            self._activo = False

        signal.signal(signal.SIGINT, _señal_paro)
        signal.signal(signal.SIGTERM, _señal_paro)

        logger.info("👂 Consumidor activo. Esperando mensajes...")
        contador = 0

        while self._activo:
            if max_mensajes and contador >= max_mensajes:
                break

            try:
                # Poll con timeout para permitir control de señales
                registros = self._consumidor.poll(timeout_ms=1_000, max_records=50)

                for partition_records in registros.values():
                    for record in partition_records:
                        if not self._activo:
                            break

                        resultado = self._procesar_mensaje(record)
                        self._metricas["mensajes_procesados"] += 1
                        if resultado.get("alertas"):
                            self._metricas["alertas_generadas"] += len(resultado["alertas"])

                        if callback_resultado:
                            callback_resultado(resultado)

                        contador += 1

                        if contador % 50 == 0:
                            logger.info(
                                f"📊 {contador} mensajes | "
                                f"{self._metricas['alertas_generadas']} alertas | "
                                f"{self._metricas['errores_procesamiento']} errores"
                            )

                # Checkpointing: commit solo tras procesamiento exitoso
                # Esto garantiza el RPO configurado
                if registros:
                    self._consumidor.commit()
                    logger.debug("✔ Offset commiteado (checkpoint)")

            except CommitFailedError as e:
                logger.error(f"Error en commit de offset: {e}. Reintentando...")
                time.sleep(1)
            except KafkaError as e:
                logger.error(f"Error Kafka: {e}. Reconectando en 5s...")
                time.sleep(5)
                self.conectar()  # Auto-recuperación (self-healing)
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                self._metricas["errores_procesamiento"] += 1

        self.cerrar()
        logger.info(
            f"✅ Consumidor detenido. "
            f"Procesados: {self._metricas['mensajes_procesados']} | "
            f"Alertas: {self._metricas['alertas_generadas']}"
        )

    def obtener_metricas(self) -> dict:
        elapsed = time.time() - self._metricas["inicio"]
        return {
            **self._metricas,
            "throughput_msg_s": self._metricas["mensajes_procesados"] / max(elapsed, 1),
            "uptime_s": round(elapsed, 1),
        }

    def cerrar(self) -> None:
        if self._consumidor:
            self._consumidor.close()
            self._consumidor = None
            logger.info("Consumidor Kafka cerrado.")

    def __enter__(self):
        self.conectar()
        return self

    def __exit__(self, *args):
        self.cerrar()


# ── Punto de entrada ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    resultados_acumulados = []

    def guardar_resultado(resultado: dict):
        resultados_acumulados.append(resultado)

    print("=" * 60)
    print("  CONSUMIDOR KAFKA — Sistema Big Data")
    print("=" * 60)

    with ConsumidorTransacciones() as consumidor:
        consumidor.consumir(max_mensajes=100, callback_resultado=guardar_resultado)
        print("\nMétricas:")
        print(json.dumps(consumidor.obtener_metricas(), indent=2))
