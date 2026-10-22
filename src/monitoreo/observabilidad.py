"""
observabilidad.py
=================
Sistema de observabilidad para arquitecturas Big Data.

Los cuatro pilares de observabilidad implementados:
1. Métricas  — Series de tiempo de rendimiento, saturación y errores.
2. Logs      — Registro detallado de eventos para diagnóstico.
3. Trazas    — Seguimiento del recorrido de una solicitud entre servicios.
4. Alertas   — Umbrales que anticipan degradaciones antes de que sean incidentes.

Este módulo simula una capa de observabilidad completa basada en
el patrón RED (Rate, Errors, Duration) y USE (Utilization, Saturation, Errors).
"""

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Métricas ───────────────────────────────────────────────────────────────────
class Contador:
    """Métrica de tipo contador (solo incrementa)."""
    def __init__(self, nombre: str, descripcion: str = ""):
        self.nombre = nombre
        self.descripcion = descripcion
        self._valor = 0
        self._lock = threading.Lock()

    def incrementar(self, delta: int = 1):
        with self._lock:
            self._valor += delta

    @property
    def valor(self) -> int:
        return self._valor

    def to_dict(self) -> dict:
        return {"nombre": self.nombre, "tipo": "contador", "valor": self._valor}


class Medidor:
    """Métrica de tipo medidor (sube y baja)."""
    def __init__(self, nombre: str, descripcion: str = ""):
        self.nombre = nombre
        self.descripcion = descripcion
        self._valor = 0.0
        self._lock = threading.Lock()

    def establecer(self, valor: float):
        with self._lock:
            self._valor = valor

    def incrementar(self, delta: float = 1.0):
        with self._lock:
            self._valor += delta

    def decrementar(self, delta: float = 1.0):
        with self._lock:
            self._valor = max(0, self._valor - delta)

    @property
    def valor(self) -> float:
        return self._valor

    def to_dict(self) -> dict:
        return {"nombre": self.nombre, "tipo": "medidor", "valor": self._valor}


class Histograma:
    """Métrica de tipo histograma para distribuciones (latencias, tamaños)."""
    def __init__(self, nombre: str, cubetas: List[float] = None):
        self.nombre = nombre
        self._cubetas = cubetas or [5, 10, 25, 50, 100, 250, 500, 1000]
        self._observaciones: deque = deque(maxlen=10_000)
        self._lock = threading.Lock()

    def observar(self, valor: float):
        with self._lock:
            self._observaciones.append(valor)

    def estadisticas(self) -> dict:
        with self._lock:
            if not self._observaciones:
                return {}
            obs = sorted(self._observaciones)
            n = len(obs)
            return {
                "nombre": self.nombre,
                "count": n,
                "suma": round(sum(obs), 3),
                "promedio": round(sum(obs) / n, 3),
                "p50": round(obs[int(n * 0.50)], 3),
                "p90": round(obs[int(n * 0.90)], 3),
                "p95": round(obs[int(n * 0.95)], 3),
                "p99": round(obs[int(n * 0.99)], 3),
                "max": round(max(obs), 3),
                "min": round(min(obs), 3),
            }


# ── Trazas distribuidas ────────────────────────────────────────────────────────
class Traza:
    """Representa el seguimiento de una solicitud a través de múltiples servicios."""

    def __init__(self, id_traza: str, operacion: str, servicio: str):
        self.id_traza = id_traza
        self.operacion = operacion
        self.servicio = servicio
        self._spans: List[dict] = []
        self._inicio = time.perf_counter()
        self._fin: Optional[float] = None
        self._error: Optional[str] = None

    def agregar_span(self, nombre: str, duracion_ms: float, metadatos: dict = None):
        """Agrega un span (segmento de tiempo en un servicio) a la traza."""
        self._spans.append({
            "nombre": nombre,
            "duracion_ms": round(duracion_ms, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadatos": metadatos or {},
        })

    def finalizar(self, error: Optional[str] = None):
        self._fin = time.perf_counter()
        self._error = error

    @property
    def duracion_ms(self) -> float:
        fin = self._fin or time.perf_counter()
        return round((fin - self._inicio) * 1000, 2)

    def to_dict(self) -> dict:
        return {
            "id_traza": self.id_traza,
            "operacion": self.operacion,
            "servicio": self.servicio,
            "duracion_total_ms": self.duracion_ms,
            "estado": "ERROR" if self._error else "OK",
            "error": self._error,
            "spans": self._spans,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ── Alertas ────────────────────────────────────────────────────────────────────
class ReglaAlerta:
    """Define un umbral de alerta proactiva."""

    def __init__(
        self,
        nombre: str,
        condicion: Callable[[dict], bool],
        mensaje: str,
        severidad: str = "WARNING",
    ):
        self.nombre = nombre
        self.condicion = condicion
        self.mensaje = mensaje
        self.severidad = severidad
        self.disparada_en: Optional[str] = None


# ── Sistema de observabilidad ──────────────────────────────────────────────────
class SistemaObservabilidad:
    """
    Sistema centralizado de observabilidad para el pipeline Big Data.

    Integra métricas, logs estructurados, trazas distribuidas y alertas proactivas.
    En producción, exportaría a Prometheus + Grafana + Jaeger + PagerDuty.
    """

    def __init__(self, nombre_servicio: str = "bigdata-pipeline"):
        self.nombre_servicio = nombre_servicio
        self._inicio = datetime.now(timezone.utc)

        # ── Métricas RED (Rate, Errors, Duration)
        self.mensajes_kafka_procesados = Contador("kafka_mensajes_procesados")
        self.errores_procesamiento = Contador("errores_procesamiento_total")
        self.alertas_fraude = Contador("alertas_fraude_total")
        self.registros_batch = Contador("batch_registros_procesados")
        self.fallos_circuit_breaker = Contador("circuit_breaker_fallos")

        # ── Métricas USE (Utilization, Saturation, Errors)
        self.conexiones_activas = Medidor("conexiones_activas")
        self.registros_en_cola = Medidor("registros_en_cola")
        self.latencia_ms = Histograma("latencia_procesamiento_ms")
        self.tamaño_lote = Histograma("tamaño_lote_registros")

        # ── Trazas
        self._trazas: deque = deque(maxlen=1_000)

        # ── Alertas
        self._reglas_alerta = self._configurar_alertas()
        self._alertas_disparadas: List[dict] = []

        # ── Logs estructurados (buffer circular)
        self._logs: deque = deque(maxlen=5_000)

        # ── Hilo de recolección de métricas
        self._activo = False
        self._hilo_metricas: Optional[threading.Thread] = None

        logger.info(f"🔍 Sistema de Observabilidad iniciado: '{nombre_servicio}'")

    def _configurar_alertas(self) -> List[ReglaAlerta]:
        """Configura reglas de alerta proactivas."""
        return [
            ReglaAlerta(
                nombre="alta_tasa_error",
                condicion=lambda m: m.get("tasa_error_pct", 0) > 5.0,
                mensaje="Tasa de error supera el 5%",
                severidad="CRITICAL",
            ),
            ReglaAlerta(
                nombre="latencia_alta",
                condicion=lambda m: m.get("latencia_p99_ms", 0) > 500,
                mensaje="Latencia P99 supera 500ms",
                severidad="WARNING",
            ),
            ReglaAlerta(
                nombre="cola_saturada",
                condicion=lambda m: m.get("registros_en_cola", 0) > 10_000,
                mensaje="Cola de mensajes saturada (>10K registros)",
                severidad="WARNING",
            ),
            ReglaAlerta(
                nombre="fraude_masivo",
                condicion=lambda m: m.get("tasa_fraude_reciente_pct", 0) > 10.0,
                mensaje="Tasa de fraude inusualmente alta (>10%)",
                severidad="CRITICAL",
            ),
        ]

    # ── API de registro ────────────────────────────────────────────────────────
    def registrar_log(self, nivel: str, mensaje: str, contexto: dict = None):
        """Registra un evento con contexto estructurado (log estructurado)."""
        entrada = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "nivel": nivel,
            "servicio": self.nombre_servicio,
            "mensaje": mensaje,
            "contexto": contexto or {},
        }
        self._logs.append(entrada)
        log_fn = getattr(logger, nivel.lower(), logger.info)
        log_fn(f"[{self.nombre_servicio}] {mensaje} | ctx={contexto}")

    def iniciar_traza(self, operacion: str, id_traza: Optional[str] = None) -> Traza:
        """Inicia el seguimiento de una operación distribuida."""
        import uuid
        traza = Traza(
            id_traza=id_traza or str(uuid.uuid4())[:8],
            operacion=operacion,
            servicio=self.nombre_servicio,
        )
        return traza

    def finalizar_traza(self, traza: Traza, error: Optional[str] = None):
        """Finaliza y almacena una traza."""
        traza.finalizar(error)
        self._trazas.append(traza.to_dict())
        self.latencia_ms.observar(traza.duracion_ms)
        if error:
            self.errores_procesamiento.incrementar()
            self.registrar_log("ERROR", f"Traza fallida: {traza.operacion}", {"error": error})

    def evaluar_alertas(self, metricas_actuales: dict) -> List[dict]:
        """Evalúa las reglas de alerta y dispara las que correspondan."""
        nuevas_alertas = []
        for regla in self._reglas_alerta:
            try:
                if regla.condicion(metricas_actuales):
                    alerta = {
                        "nombre": regla.nombre,
                        "severidad": regla.severidad,
                        "mensaje": regla.mensaje,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "metricas": metricas_actuales,
                    }
                    self._alertas_disparadas.append(alerta)
                    nuevas_alertas.append(alerta)
                    nivel = "critical" if regla.severidad == "CRITICAL" else "warning"
                    logger.log(
                        logging.CRITICAL if nivel == "critical" else logging.WARNING,
                        f"🚨 ALERTA [{regla.severidad}] — {regla.mensaje}"
                    )
            except Exception:
                pass
        return nuevas_alertas

    # ── Snapshots de métricas ──────────────────────────────────────────────────
    def snapshot(self) -> dict:
        """Captura el estado actual de todas las métricas."""
        lat_stats = self.latencia_ms.estadisticas()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "servicio": self.nombre_servicio,
            "uptime_s": round(
                (datetime.now(timezone.utc) - self._inicio).total_seconds(), 1
            ),
            "contadores": {
                "mensajes_kafka": self.mensajes_kafka_procesados.valor,
                "errores": self.errores_procesamiento.valor,
                "alertas_fraude": self.alertas_fraude.valor,
                "registros_batch": self.registros_batch.valor,
            },
            "medidores": {
                "conexiones_activas": self.conexiones_activas.valor,
                "registros_en_cola": self.registros_en_cola.valor,
            },
            "latencia": lat_stats,
            "tasa_error_pct": round(
                self.errores_procesamiento.valor
                / max(self.mensajes_kafka_procesados.valor, 1) * 100, 3
            ),
            "trazas_recientes": list(self._trazas)[-5:],
            "alertas_activas": self._alertas_disparadas[-10:],
            "logs_recientes": list(self._logs)[-10:],
        }

    def imprimir_dashboard(self) -> None:
        """Imprime un dashboard de texto en consola."""
        s = self.snapshot()
        print("\n" + "═" * 65)
        print(f"  📊 DASHBOARD — {self.nombre_servicio.upper()}")
        print(f"  {s['timestamp']}  |  Uptime: {s['uptime_s']}s")
        print("═" * 65)
        print(f"  Mensajes Kafka:  {s['contadores']['mensajes_kafka']:>8,}")
        print(f"  Errores:         {s['contadores']['errores']:>8,}  ({s['tasa_error_pct']:.2f}%)")
        print(f"  Alertas fraude:  {s['contadores']['alertas_fraude']:>8,}")
        print(f"  Conexiones:      {s['medidores']['conexiones_activas']:>8.0f}")
        print(f"  En cola:         {s['medidores']['registros_en_cola']:>8.0f}")
        if s['latencia']:
            lat = s['latencia']
            print(f"  Latencia P50:    {lat.get('p50', 0):>7.1f}ms")
            print(f"  Latencia P99:    {lat.get('p99', 0):>7.1f}ms")
        print(f"  Alertas activas: {len(s['alertas_activas'])}")
        print("═" * 65)


# ── Punto de entrada ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import random
    logging.basicConfig(level=logging.WARNING)

    obs = SistemaObservabilidad("pipeline-transacciones")

    print("=" * 65)
    print("  SISTEMA DE OBSERVABILIDAD — Demo")
    print("=" * 65)

    # Simular carga de trabajo
    for i in range(100):
        traza = obs.iniciar_traza(f"procesar_transaccion_{i}")
        traza.agregar_span("kafka_consume", random.uniform(2, 15))
        traza.agregar_span("validar_datos", random.uniform(1, 5))
        traza.agregar_span("detectar_fraude", random.uniform(10, 50))

        if random.random() < 0.03:
            obs.finalizar_traza(traza, error="TimeoutError")
        else:
            obs.finalizar_traza(traza)
            obs.mensajes_kafka_procesados.incrementar()

        if random.random() < 0.05:
            obs.alertas_fraude.incrementar()

        obs.conexiones_activas.establecer(random.randint(3, 15))
        obs.registros_en_cola.establecer(random.randint(0, 8000))

    # Evaluar alertas
    snap = obs.snapshot()
    alertas = obs.evaluar_alertas({
        **snap["medidores"],
        "tasa_error_pct": snap["tasa_error_pct"],
        "latencia_p99_ms": snap["latencia"].get("p99", 0),
        "tasa_fraude_reciente_pct": 2.0,
    })

    obs.imprimir_dashboard()

    if alertas:
        print(f"\n🚨 {len(alertas)} alerta(s) disparada(s):")
        for a in alertas:
            print(f"  [{a['severidad']}] {a['mensaje']}")
    else:
        print("\n✅ Sin alertas activas.")
