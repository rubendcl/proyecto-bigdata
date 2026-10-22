"""
circuit_breaker.py
==================
Implementación del patrón Circuit Breaker para arquitecturas de alta disponibilidad.

El Circuit Breaker es un patrón de resiliencia que:
1. Detecta fallos repetidos en un servicio
2. "Abre" el circuito para evitar fallos en cascada
3. Permite recuperación automática tras un tiempo de espera
4. Redirige el tráfico a componentes sanos (failover)

Estados del circuito:
  CERRADO  → Operación normal. Las llamadas pasan.
  ABIERTO  → Fallo detectado. Las llamadas se rechazan inmediatamente.
  SEMI-ABIERTO → Período de prueba. Se permite una llamada de prueba.
"""

import logging
import threading
import time
from collections import deque
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EstadoCircuito(Enum):
    CERRADO = "CERRADO"          # Normal: llamadas permitidas
    ABIERTO = "ABIERTO"          # Fallo: llamadas rechazadas
    SEMI_ABIERTO = "SEMI_ABIERTO"  # Prueba: una llamada permitida


class CircuitoBiertoError(Exception):
    """Excepción lanzada cuando el circuito está abierto."""
    pass


class CircuitBreaker:
    """
    Implementación thread-safe del patrón Circuit Breaker.

    Parámetros
    ----------
    nombre         : Identificador del servicio protegido.
    umbral_fallos  : Número de fallos consecutivos para abrir el circuito.
    timeout_s      : Segundos en estado ABIERTO antes de intentar semi-apertura.
    ventana_s      : Ventana de tiempo para contabilizar fallos.
    """

    def __init__(
        self,
        nombre: str,
        umbral_fallos: int = 5,
        timeout_s: int = 30,
        ventana_s: int = 60,
    ):
        self.nombre = nombre
        self.umbral_fallos = umbral_fallos
        self.timeout_s = timeout_s
        self.ventana_s = ventana_s

        self._estado = EstadoCircuito.CERRADO
        self._lock = threading.Lock()
        self._fallos_recientes: deque = deque()  # timestamps de fallos
        self._ultimo_fallo_timestamp: float = 0.0
        self._intentos_semi_abierto: int = 0

        # Métricas de observabilidad
        self._total_llamadas = 0
        self._total_exitos = 0
        self._total_fallos = 0
        self._total_rechazos = 0  # Llamadas rechazadas por circuito abierto
        self._historial_estado: list = []

        logger.info(f"🔌 CircuitBreaker '{nombre}' iniciado (umbral={umbral_fallos}, timeout={timeout_s}s)")

    # ── Propiedades ────────────────────────────────────────────────────────────
    @property
    def estado(self) -> EstadoCircuito:
        return self._estado

    @property
    def esta_disponible(self) -> bool:
        """Retorna True si el circuito permite llamadas."""
        with self._lock:
            return self._evaluar_estado() != EstadoCircuito.ABIERTO

    def _evaluar_estado(self) -> EstadoCircuito:
        """
        Evalúa y transiciona el estado del circuito.
        Debe llamarse dentro de un bloque lock.
        """
        ahora = time.monotonic()

        if self._estado == EstadoCircuito.ABIERTO:
            # ¿Ha pasado suficiente tiempo para intentar recuperación?
            tiempo_desde_fallo = ahora - self._ultimo_fallo_timestamp
            if tiempo_desde_fallo >= self.timeout_s:
                self._transicionar(EstadoCircuito.SEMI_ABIERTO)

        elif self._estado == EstadoCircuito.CERRADO:
            # Limpiar fallos fuera de la ventana de tiempo
            while self._fallos_recientes and (ahora - self._fallos_recientes[0]) > self.ventana_s:
                self._fallos_recientes.popleft()

            # ¿Se superó el umbral de fallos?
            if len(self._fallos_recientes) >= self.umbral_fallos:
                self._transicionar(EstadoCircuito.ABIERTO)

        return self._estado

    def _transicionar(self, nuevo_estado: EstadoCircuito) -> None:
        """Registra la transición de estado con métricas."""
        estado_anterior = self._estado
        self._estado = nuevo_estado
        self._historial_estado.append({
            "de": estado_anterior.value,
            "a": nuevo_estado.value,
            "timestamp": time.time(),
        })
        logger.warning(
            f"⚡ CircuitBreaker '{self.nombre}': "
            f"{estado_anterior.value} → {nuevo_estado.value}"
        )

    # ── Ejecución protegida ────────────────────────────────────────────────────
    def ejecutar(self, operacion: Callable, *args, **kwargs) -> Any:
        """
        Ejecuta una operación protegida por el Circuit Breaker.

        Si el circuito está ABIERTO, lanza CircuitoBiertoError inmediatamente
        sin intentar la operación (evita fallos en cascada).

        Parámetros
        ----------
        operacion : Callable que puede fallar (llamada a servicio externo).

        Retorna
        -------
        El resultado de la operación si tiene éxito.

        Lanza
        -----
        CircuitoBiertoError : Si el circuito está ABIERTO.
        Exception           : Si la operación falla y el circuito se abre.
        """
        with self._lock:
            estado_actual = self._evaluar_estado()
            self._total_llamadas += 1

            if estado_actual == EstadoCircuito.ABIERTO:
                self._total_rechazos += 1
                raise CircuitoBiertoError(
                    f"Circuito '{self.nombre}' ABIERTO. "
                    f"Servicio no disponible. Espere {self.timeout_s}s."
                )

            # SEMI_ABIERTO: solo permitir una llamada de prueba
            if estado_actual == EstadoCircuito.SEMI_ABIERTO:
                self._intentos_semi_abierto += 1

        # Ejecutar la operación FUERA del lock (no bloquear otros hilos)
        try:
            resultado = operacion(*args, **kwargs)
            self._registrar_exito()
            return resultado
        except Exception as e:
            self._registrar_fallo(e)
            raise

    def _registrar_exito(self) -> None:
        """Registra una llamada exitosa y cierra el circuito si estaba semi-abierto."""
        with self._lock:
            self._total_exitos += 1
            if self._estado == EstadoCircuito.SEMI_ABIERTO:
                self._fallos_recientes.clear()
                self._transicionar(EstadoCircuito.CERRADO)
                logger.info(f"✅ CircuitBreaker '{self.nombre}': Recuperado. Circuito CERRADO.")

    def _registrar_fallo(self, error: Exception) -> None:
        """Registra un fallo y potencialmente abre el circuito."""
        with self._lock:
            self._total_fallos += 1
            self._ultimo_fallo_timestamp = time.monotonic()
            self._fallos_recientes.append(self._ultimo_fallo_timestamp)

            logger.warning(
                f"⚠️  CircuitBreaker '{self.nombre}': Fallo #{len(self._fallos_recientes)} "
                f"({type(error).__name__}: {error})"
            )

            if self._estado == EstadoCircuito.SEMI_ABIERTO:
                self._transicionar(EstadoCircuito.ABIERTO)

    # ── Decorador ──────────────────────────────────────────────────────────────
    def proteger(self, fn: Callable) -> Callable:
        """Decorador para proteger automáticamente una función."""
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return self.ejecutar(fn, *args, **kwargs)
        return wrapper

    # ── Métricas ───────────────────────────────────────────────────────────────
    def obtener_metricas(self) -> dict:
        """Retorna métricas de observabilidad del Circuit Breaker."""
        tasa_exito = (
            self._total_exitos / self._total_llamadas * 100
            if self._total_llamadas > 0
            else 100.0
        )
        return {
            "nombre": self.nombre,
            "estado": self._estado.value,
            "total_llamadas": self._total_llamadas,
            "total_exitos": self._total_exitos,
            "total_fallos": self._total_fallos,
            "total_rechazos": self._total_rechazos,
            "tasa_exito_pct": round(tasa_exito, 2),
            "fallos_en_ventana": len(self._fallos_recientes),
            "umbral_fallos": self.umbral_fallos,
            "historial_transiciones": self._historial_estado[-10:],  # Últimas 10
        }

    def resetear(self) -> None:
        """Resetea el circuito manualmente (operación de administración)."""
        with self._lock:
            self._fallos_recientes.clear()
            self._transicionar(EstadoCircuito.CERRADO)
            logger.info(f"🔄 CircuitBreaker '{self.nombre}' reseteado manualmente.")

    def __repr__(self) -> str:
        return f"CircuitBreaker(nombre='{self.nombre}', estado={self._estado.value})"


# ── Demo ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    import random

    cb = CircuitBreaker("servicio-pagos", umbral_fallos=3, timeout_s=5)

    def servicio_externo_inestable():
        """Simula un servicio externo que falla el 70% del tiempo."""
        if random.random() < 0.7:
            raise ConnectionError("Servicio no disponible")
        return {"status": "OK", "tiempo_ms": random.randint(50, 200)}

    print("=== Demo Circuit Breaker ===\n")

    for i in range(1, 16):
        try:
            resultado = cb.ejecutar(servicio_externo_inestable)
            print(f"[{i:2d}] ✅ Éxito: {resultado}")
        except CircuitoBiertoError as e:
            print(f"[{i:2d}] 🔴 CIRCUITO ABIERTO: {e}")
        except ConnectionError as e:
            print(f"[{i:2d}] ❌ Fallo: {e} | Estado: {cb.estado.value}")

        time.sleep(0.3)
        if i == 10:
            print("\n⏳ Esperando que el circuito se recupere (5s)...")
            time.sleep(5)

    print("\n📊 Métricas finales:")
    import json
    print(json.dumps(cb.obtener_metricas(), indent=2))
