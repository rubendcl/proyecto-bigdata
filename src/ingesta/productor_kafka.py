"""
productor_kafka.py
==================
Productor Kafka para la capa de ingesta en tiempo real.
Implementa la capa de Velocidad de la Arquitectura Lambda y
el pipeline único de la Arquitectura Kappa.

Conceptos demostrados:
- Procesamiento en flujo (streaming)
- Velocidad: eventos procesados en milisegundos
- Particionamiento de tópicos Kafka
- Serialización de mensajes
"""

import json
import logging
import time
import signal
import sys
from typing import Optional, Callable, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from src.config import Configuracion
from src.ingesta.simulador_transacciones import flujo_continuo, generar_transaccion
from src.resiliencia.circuit_breaker import CircuitBreaker, EstadoCircuito

logger = logging.getLogger(__name__)
cfg = Configuracion()


class ProductorTransacciones:
    """
    Productor Kafka con tolerancia a fallos y Circuit Breaker integrado.

    Características de alta disponibilidad:
    - Reintentos automáticos ante fallos de red
    - Circuit Breaker para evitar fallos en cascada
    - Métricas de producción en tiempo real
    - Serialización segura de mensajes
    """

    def __init__(self):
        self._productor: Optional[KafkaProducer] = None
        self._circuit_breaker = CircuitBreaker(
            nombre="kafka-productor",
            umbral_fallos=cfg.resiliencia.umbral_fallos_cb,
            timeout_s=cfg.resiliencia.timeout_circuito_abierto_s,
        )
        self._metricas = {
            "mensajes_enviados": 0,
            "mensajes_fallidos": 0,
            "bytes_enviados": 0,
            "inicio_timestamp": time.time(),
        }
        self._activo = False

    # ── Conexión ───────────────────────────────────────────────────────────────
    def conectar(self) -> bool:
        """Establece conexión con el clúster Kafka con reintentos."""
        for intento in range(1, cfg.recuperacion.max_reintentos + 1):
            try:
                logger.info(f"Conectando a Kafka [{intento}/{cfg.recuperacion.max_reintentos}]...")
                self._productor = KafkaProducer(
                    bootstrap_servers=cfg.kafka.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    # Alta disponibilidad: acks=all garantiza que todas las réplicas recibieron el mensaje
                    acks="all",
                    # Reintentos ante fallos transitorios (tolerancia a fallos)
                    retries=3,
                    retry_backoff_ms=1_000,
                    # Compresión para optimizar throughput (Volumen)
                    compression_type="gzip",
                    # Lotes para mayor eficiencia
                    batch_size=16_384,
                    linger_ms=10,
                    request_timeout_ms=cfg.kafka.timeout_ms,
                )
                logger.info("✅ Conexión a Kafka establecida.")
                return True

            except NoBrokersAvailable:
                logger.warning(f"❌ No hay brokers disponibles. Reintentando en 5s...")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error al conectar: {e}")
                time.sleep(5)

        logger.error("❌ No se pudo conectar a Kafka después de todos los reintentos.")
        return False

    # ── Envío de mensajes ──────────────────────────────────────────────────────
    def _enviar_mensaje(
        self,
        topico: str,
        mensaje: dict,
        clave: Optional[str] = None,
    ) -> bool:
        """
        Envía un mensaje al tópico Kafka usando el Circuit Breaker.
        La clave de partición (shard_key) asegura que transacciones
        del mismo shard van siempre a la misma partición Kafka.
        """
        if not self._productor:
            logger.error("Productor no inicializado.")
            return False

        def _operacion_envio():
            future = self._productor.send(
                topico,
                key=clave or mensaje.get("shard_key"),
                value=mensaje,
            )
            record_metadata = future.get(timeout=10)
            tamaño_bytes = len(json.dumps(mensaje).encode("utf-8"))
            self._metricas["mensajes_enviados"] += 1
            self._metricas["bytes_enviados"] += tamaño_bytes
            logger.debug(
                f"✉️  Enviado → {topico} | "
                f"partición={record_metadata.partition} | "
                f"offset={record_metadata.offset} | "
                f"{tamaño_bytes} bytes"
            )
            return True

        try:
            return self._circuit_breaker.ejecutar(_operacion_envio)
        except Exception as e:
            self._metricas["mensajes_fallidos"] += 1
            logger.error(f"Error al enviar mensaje: {e}")
            return False

    # ── Producción continua ────────────────────────────────────────────────────
    def producir_streaming(
        self,
        intervalo_s: float = 0.5,
        max_mensajes: Optional[int] = None,
        callback_progreso: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """
        Produce transacciones de forma continua hacia Kafka (modo Streaming).
        Representa la capa de velocidad de Arquitectura Lambda / Kappa.

        Parámetros
        ----------
        intervalo_s       : Tiempo entre mensajes (simula velocidad de ingesta).
        max_mensajes      : Límite de mensajes (None = indefinido).
        callback_progreso : Función opcional para monitoreo externo.
        """
        if not self._productor and not self.conectar():
            raise RuntimeError("No se puede iniciar streaming sin conexión Kafka.")

        self._activo = True
        contador = 0

        def _manejar_señal(sig, frame):
            logger.info("\n🛑 Señal recibida. Deteniendo productor...")
            self._activo = False

        signal.signal(signal.SIGINT, _manejar_señal)
        signal.signal(signal.SIGTERM, _manejar_señal)

        logger.info(f"🚀 Iniciando producción streaming (intervalo={intervalo_s}s)...")

        for transaccion in flujo_continuo(intervalo_segundos=intervalo_s):
            if not self._activo:
                break
            if max_mensajes and contador >= max_mensajes:
                break

            # Enrutar según tipo: fraudes potenciales → tópico de alertas
            topico = (
                cfg.kafka.topico_alertas
                if transaccion.get("es_fraude_potencial")
                else cfg.kafka.topico_transacciones
            )

            exito = self._enviar_mensaje(topico, transaccion)

            if callback_progreso:
                callback_progreso({
                    "transaccion": transaccion,
                    "exito": exito,
                    "metricas": self.obtener_metricas(),
                })

            contador += 1

            # Log periódico cada 100 mensajes
            if contador % 100 == 0:
                m = self.obtener_metricas()
                logger.info(
                    f"📊 Progreso: {m['mensajes_enviados']} enviados | "
                    f"{m['mensajes_fallidos']} fallidos | "
                    f"{m['throughput_msg_s']:.1f} msg/s | "
                    f"Circuito: {self._circuit_breaker.estado.value}"
                )

        self.cerrar()
        logger.info(f"✅ Producción finalizada. Total: {contador} mensajes.")

    # ── Producción de lote ─────────────────────────────────────────────────────
    def producir_lote(self, transacciones: list) -> dict:
        """
        Envía un lote de transacciones (modo Batch).
        Representa la capa batch de Arquitectura Lambda.
        """
        if not self._productor and not self.conectar():
            raise RuntimeError("Sin conexión Kafka.")

        enviados, fallidos = 0, 0
        for tx in transacciones:
            if self._enviar_mensaje(cfg.kafka.topico_transacciones, tx):
                enviados += 1
            else:
                fallidos += 1

        self._productor.flush()
        logger.info(f"Lote enviado: {enviados} OK, {fallidos} fallidos de {len(transacciones)}")
        return {"enviados": enviados, "fallidos": fallidos, "total": len(transacciones)}

    # ── Métricas ───────────────────────────────────────────────────────────────
    def obtener_metricas(self) -> dict:
        """Retorna métricas de observabilidad del productor."""
        elapsed = time.time() - self._metricas["inicio_timestamp"]
        return {
            **self._metricas,
            "throughput_msg_s": self._metricas["mensajes_enviados"] / max(elapsed, 1),
            "throughput_kb_s": self._metricas["bytes_enviados"] / max(elapsed, 1) / 1024,
            "tasa_error_pct": (
                self._metricas["mensajes_fallidos"]
                / max(self._metricas["mensajes_enviados"] + self._metricas["mensajes_fallidos"], 1)
                * 100
            ),
            "estado_circuit_breaker": self._circuit_breaker.estado.value,
            "tiempo_activo_s": round(elapsed, 1),
        }

    # ── Ciclo de vida ──────────────────────────────────────────────────────────
    def cerrar(self) -> None:
        """Cierra el productor liberando recursos correctamente."""
        if self._productor:
            self._productor.flush()
            self._productor.close()
            self._productor = None
            logger.info("Productor Kafka cerrado.")

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

    print("=" * 60)
    print("  PRODUCTOR KAFKA — Sistema Big Data")
    print("=" * 60)

    with ProductorTransacciones() as productor:
        # Produce 200 mensajes de demostración
        productor.producir_streaming(
            intervalo_s=0.2,
            max_mensajes=200,
        )
        print("\nMétricas finales:")
        import json
        print(json.dumps(productor.obtener_metricas(), indent=2))
