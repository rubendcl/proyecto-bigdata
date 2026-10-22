"""
replicacion.py
==============
Simulación de estrategias de replicación de datos para alta disponibilidad.

Estrategias implementadas:
  1. Replicación Síncrona  — Máxima consistencia, mayor latencia.
                             La escritura se confirma solo cuando TODAS las
                             réplicas la reciben.
  2. Replicación Asíncrona — Menor latencia, riesgo de pérdida ante falla.
                             La escritura se confirma en el nodo principal y
                             se propaga después.
  3. Multi-líder (Multi-master) — Alta disponibilidad geográfica.
                             Varios nodos aceptan escrituras simultáneamente;
                             requiere resolución de conflictos.

Relación con el Teorema CAP:
  Síncrona   → prioriza CP (Consistencia + Tolerancia a Particiones)
  Asíncrona  → prioriza AP (Disponibilidad + Tolerancia a Particiones)
  Multi-líder → AP con resolución eventual de conflictos (BASE vs ACID)
"""

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Tipos auxiliares ───────────────────────────────────────────────────────────
class RolNodo(Enum):
    LIDER = "LIDER"
    REPLICA = "REPLICA"
    MULTI_LIDER = "MULTI_LIDER"


class EstadoNodo(Enum):
    ACTIVO = "ACTIVO"
    CAIDO = "CAIDO"
    SINCRONIZANDO = "SINCRONIZANDO"


@dataclass
class ResultadoEscritura:
    exito: bool
    nodos_confirmados: int
    nodos_totales: int
    latencia_ms: float
    datos_perdidos: int = 0
    conflictos_resueltos: int = 0
    mensaje: str = ""


# ── Nodo de replicación ────────────────────────────────────────────────────────
class NodoReplicacion:
    """Representa un nodo del clúster de replicación."""

    def __init__(self, id_nodo: str, rol: RolNodo, latencia_red_ms: float = 5.0):
        self.id_nodo = id_nodo
        self.rol = rol
        self.estado = EstadoNodo.ACTIVO
        self.latencia_red_ms = latencia_red_ms
        self._datos: Dict[str, Any] = {}
        self._log_operaciones: List[dict] = []
        self._lock = threading.Lock()
        self._secuencia = 0          # Número de secuencia lógica (vector clock)
        self.metricas = {
            "escrituras": 0,
            "lecturas": 0,
            "conflictos": 0,
        }

    def escribir(self, clave: str, valor: Any, origen: str = "local") -> Tuple[bool, float]:
        """
        Escribe un valor en el nodo. Simula la latencia de red.
        Retorna (éxito, latencia_ms).
        """
        if self.estado == EstadoNodo.CAIDO:
            return False, 0.0

        # La variación aleatoria puede superar latencias base pequeñas (p. ej. 1 ms).
        # Una latencia de red simulada no puede ser negativa.
        latencia = max(0.0, self.latencia_red_ms + random.uniform(-2, 2))
        time.sleep(latencia / 1000)  # Simula latencia de red

        with self._lock:
            self._secuencia += 1
            self._datos[clave] = {
                "valor": valor,
                "timestamp": time.time(),
                "secuencia": self._secuencia,
                "origen": origen,
            }
            self._log_operaciones.append({
                "operacion": "WRITE",
                "clave": clave,
                "secuencia": self._secuencia,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            self.metricas["escrituras"] += 1

        return True, latencia

    def leer(self, clave: str) -> Optional[Any]:
        """Lee un valor del nodo."""
        if self.estado == EstadoNodo.CAIDO:
            return None
        self.metricas["lecturas"] += 1
        entrada = self._datos.get(clave)
        return entrada["valor"] if entrada else None

    def simular_caida(self):
        """Simula un fallo del nodo."""
        self.estado = EstadoNodo.CAIDO
        logger.warning(f"💥 Nodo '{self.id_nodo}' CAÍDO.")

    def simular_recuperacion(self):
        """Simula la recuperación del nodo."""
        self.estado = EstadoNodo.ACTIVO
        logger.info(f"✅ Nodo '{self.id_nodo}' recuperado.")

    def __repr__(self) -> str:
        return f"Nodo({self.id_nodo}, rol={self.rol.value}, estado={self.estado.value})"


# ════════════════════════════════════════════════════════════════════
# 1. REPLICACIÓN SÍNCRONA
# ════════════════════════════════════════════════════════════════════
class ReplicacionSincrona:
    """
    La escritura se confirma solo cuando TODAS las réplicas activas la reciben.

    Garantía: Máxima consistencia (CP según CAP).
    Costo: Mayor latencia (suma de todas las latencias de red).
    Uso ideal: transacciones financieras, sistemas de pago donde la
    consistencia es innegociable.
    """

    def __init__(self, num_replicas: int = 3):
        self.nodo_lider = NodoReplicacion("nodo-lider", RolNodo.LIDER, latencia_red_ms=1.0)
        self.replicas = [
            NodoReplicacion(f"replica-{i}", RolNodo.REPLICA, latencia_red_ms=10 + i * 5)
            for i in range(1, num_replicas)
        ]
        self._metricas = {"escrituras_ok": 0, "escrituras_fail": 0, "latencia_total_ms": 0.0}
        logger.info(f"Replicación Síncrona iniciada: 1 líder + {len(self.replicas)} réplicas")

    def escribir(self, clave: str, valor: Any) -> ResultadoEscritura:
        """
        Escribe en el líder y en TODAS las réplicas.
        Solo confirma cuando todas responden (acks=all).
        """
        t0 = time.perf_counter()
        nodos_activos = [r for r in self.replicas if r.estado == EstadoNodo.ACTIVO]

        if not nodos_activos:
            return ResultadoEscritura(
                exito=False, nodos_confirmados=0,
                nodos_totales=len(self.replicas),
                latencia_ms=0, mensaje="Sin réplicas disponibles"
            )

        # Paso 1: escribir en el líder
        exito_lider, lat_lider = self.nodo_lider.escribir(clave, valor)
        if not exito_lider:
            return ResultadoEscritura(exito=False, nodos_confirmados=0,
                                      nodos_totales=len(self.replicas) + 1, latencia_ms=0)

        # Paso 2: propagar SÍNCRONAMENTE a TODAS las réplicas
        confirmados = 1
        threads = []
        resultados = []

        def _propagar(replica):
            ok, lat = replica.escribir(clave, valor, origen=self.nodo_lider.id_nodo)
            resultados.append((ok, lat))

        for replica in nodos_activos:
            t = threading.Thread(target=_propagar, args=(replica,))
            threads.append(t)
            t.start()

        # ESPERAR a que TODAS las réplicas confirmen (síncrono)
        for t in threads:
            t.join(timeout=5.0)

        confirmados += sum(1 for ok, _ in resultados if ok)
        latencia_total = (time.perf_counter() - t0) * 1000

        exito = confirmados == len(nodos_activos) + 1  # Líder + todas las réplicas
        self._metricas["escrituras_ok" if exito else "escrituras_fail"] += 1
        self._metricas["latencia_total_ms"] += latencia_total

        resultado = ResultadoEscritura(
            exito=exito,
            nodos_confirmados=confirmados,
            nodos_totales=len(self.replicas) + 1,
            latencia_ms=round(latencia_total, 2),
            mensaje="OK" if exito else f"Solo {confirmados} de {len(nodos_activos)+1} confirmaron",
        )

        nivel = "info" if exito else "warning"
        getattr(logger, nivel)(
            f"[SÍNCRONA] '{clave}' → {confirmados}/{len(nodos_activos)+1} nodos | "
            f"{latencia_total:.1f}ms | {'✅' if exito else '❌'}"
        )
        return resultado

    def leer(self, clave: str) -> Any:
        """Siempre lee desde el líder (consistencia fuerte)."""
        return self.nodo_lider.leer(clave)

    def estadisticas(self) -> dict:
        total = self._metricas["escrituras_ok"] + self._metricas["escrituras_fail"]
        return {
            "tipo": "sincrona",
            **self._metricas,
            "latencia_promedio_ms": round(
                self._metricas["latencia_total_ms"] / max(total, 1), 2
            ),
            "nodos": [str(n) for n in [self.nodo_lider] + self.replicas],
        }


# ════════════════════════════════════════════════════════════════════
# 2. REPLICACIÓN ASÍNCRONA
# ════════════════════════════════════════════════════════════════════
class ReplicacionAsincrona:
    """
    La escritura se confirma en el nodo principal y se propaga después.

    Garantía: Menor latencia, eventual consistencia (AP según CAP).
    Riesgo: Pérdida de datos si el líder falla antes de propagar.
    Uso ideal: analytics, logs, sistemas donde la velocidad importa más
    que la consistencia inmediata.
    """

    def __init__(self, num_replicas: int = 3):
        self.nodo_lider = NodoReplicacion("nodo-lider-async", RolNodo.LIDER, latencia_red_ms=1.0)
        self.replicas = [
            NodoReplicacion(f"replica-async-{i}", RolNodo.REPLICA, latencia_red_ms=20 + i * 10)
            for i in range(1, num_replicas)
        ]
        self._cola_propagacion: List[dict] = []
        self._cola_lock = threading.Lock()
        self._metricas = {
            "escrituras_ok": 0, "latencia_total_ms": 0.0,
            "datos_en_cola": 0, "datos_perdidos": 0,
        }
        self._hilo_propagacion = threading.Thread(
            target=self._propagar_en_background, daemon=True
        )
        self._hilo_propagacion.start()
        logger.info(f"Replicación Asíncrona iniciada: 1 líder + {len(self.replicas)} réplicas")

    def escribir(self, clave: str, valor: Any) -> ResultadoEscritura:
        """
        Escribe SOLO en el líder y confirma inmediatamente.
        La propagación a réplicas ocurre en segundo plano.
        """
        t0 = time.perf_counter()
        exito, lat_lider = self.nodo_lider.escribir(clave, valor)
        latencia_ms = (time.perf_counter() - t0) * 1000

        if exito:
            # Encolar propagación asíncrona
            with self._cola_lock:
                self._cola_propagacion.append({"clave": clave, "valor": valor, "timestamp": time.time()})
                self._metricas["datos_en_cola"] += 1

            self._metricas["escrituras_ok"] += 1
            self._metricas["latencia_total_ms"] += latencia_ms

        logger.debug(
            f"[ASÍNCRONA] '{clave}' confirmado en líder en {latencia_ms:.1f}ms "
            f"(propagación pendiente en {len(self.replicas)} réplicas)"
        )
        return ResultadoEscritura(
            exito=exito, nodos_confirmados=1 if exito else 0,
            nodos_totales=len(self.replicas) + 1,
            latencia_ms=round(latencia_ms, 2),
            mensaje="Confirmado en líder. Réplicas: propagación asíncrona pendiente.",
        )

    def _propagar_en_background(self):
        """Hilo de propagación asíncrona de datos a las réplicas."""
        while True:
            with self._cola_lock:
                if self._cola_propagacion:
                    item = self._cola_propagacion.pop(0)
                    self._metricas["datos_en_cola"] = len(self._cola_propagacion)
                else:
                    item = None

            if item:
                for replica in self.replicas:
                    if replica.estado == EstadoNodo.ACTIVO:
                        replica.escribir(item["clave"], item["valor"], origen="propagacion-async")
            time.sleep(0.05)

    def leer_consistencia_eventual(self, clave: str, preferir_local: bool = True) -> Tuple[Any, str]:
        """
        Lectura con consistencia eventual: puede leer desde cualquier réplica.
        Retorna (valor, nodo_origen).
        """
        nodos = ([self.nodo_lider] + self.replicas) if preferir_local else (self.replicas + [self.nodo_lider])
        for nodo in nodos:
            if nodo.estado == EstadoNodo.ACTIVO:
                valor = nodo.leer(clave)
                if valor is not None:
                    return valor, nodo.id_nodo
        return None, "ninguno"

    def estadisticas(self) -> dict:
        total = self._metricas["escrituras_ok"]
        return {
            "tipo": "asincrona",
            **self._metricas,
            "latencia_promedio_ms": round(
                self._metricas["latencia_total_ms"] / max(total, 1), 2
            ),
        }


# ════════════════════════════════════════════════════════════════════
# 3. REPLICACIÓN MULTI-LÍDER (MULTI-MASTER)
# ════════════════════════════════════════════════════════════════════
class ReplicacionMultiLider:
    """
    Varios nodos aceptan escrituras simultáneamente.
    Alta disponibilidad geográfica, requiere resolución de conflictos.

    Garantía: Máxima disponibilidad (AP según CAP).
    Riesgo: Conflictos de escritura concurrente.
    Resolución: Last-Write-Wins (LWW) basado en timestamp.
    Uso ideal: sistemas geo-distribuidos, edición colaborativa, IoT.
    """

    def __init__(self, regiones: List[str] = None):
        regiones = regiones or ["region-a", "region-b", "region-c"]
        self.lideres = {
            region: NodoReplicacion(f"lider-{region}", RolNodo.MULTI_LIDER, latencia_red_ms=5.0)
            for region in regiones
        }
        self._metricas = {"escrituras": 0, "conflictos_detectados": 0, "conflictos_resueltos": 0}
        logger.info(f"Multi-Líder iniciado: {len(regiones)} líderes regionales ({', '.join(regiones)})")

    def escribir_en_region(self, region: str, clave: str, valor: Any) -> ResultadoEscritura:
        """Escribe en el líder de una región específica."""
        lider = self.lideres.get(region)
        if not lider or lider.estado == EstadoNodo.CAIDO:
            # Failover: usar otro líder disponible
            for otra_region, otro_lider in self.lideres.items():
                if otro_lider.estado == EstadoNodo.ACTIVO:
                    logger.warning(f"Failover: región '{region}' no disponible → usando '{otra_region}'")
                    lider = otro_lider
                    break
            else:
                return ResultadoEscritura(exito=False, nodos_confirmados=0, nodos_totales=len(self.lideres), latencia_ms=0)

        t0 = time.perf_counter()
        exito, lat = lider.escribir(clave, valor, origen=region)
        latencia_ms = (time.perf_counter() - t0) * 1000

        if exito:
            self._metricas["escrituras"] += 1
            # Propagar a los otros líderes (cross-region replication)
            threading.Thread(
                target=self._replicar_entre_lideres,
                args=(region, clave, valor),
                daemon=True,
            ).start()

        return ResultadoEscritura(
            exito=exito, nodos_confirmados=1 if exito else 0,
            nodos_totales=len(self.lideres),
            latencia_ms=round(latencia_ms, 2),
            mensaje=f"Escrito en líder-{region}. Propagación cross-region en curso.",
        )

    def _replicar_entre_lideres(self, region_origen: str, clave: str, valor: Any):
        """Propaga la escritura a los demás líderes y resuelve conflictos."""
        for region, lider in self.lideres.items():
            if region == region_origen or lider.estado == EstadoNodo.CAIDO:
                continue

            # Detectar conflicto: ¿ya existe un valor más reciente?
            valor_existente = lider.leer(clave)
            if valor_existente and isinstance(valor, dict) and isinstance(valor_existente, dict):
                ts_nuevo = valor.get("timestamp_unix", 0)
                ts_existente = valor_existente.get("timestamp_unix", 0)
                if ts_existente > ts_nuevo:
                    self._metricas["conflictos_detectados"] += 1
                    # Resolución LWW: gana el más reciente
                    logger.debug(
                        f"Conflicto en '{clave}' entre {region_origen} y {region}. "
                        f"LWW: conservando escritura más reciente."
                    )
                    self._metricas["conflictos_resueltos"] += 1
                    continue

            lider.escribir(clave, valor, origen=f"cross-region-{region_origen}")

    def estadisticas(self) -> dict:
        return {
            "tipo": "multi_lider",
            "num_lideres": len(self.lideres),
            "lideres": {r: str(n) for r, n in self.lideres.items()},
            **self._metricas,
        }


# ── Comparador de estrategias ──────────────────────────────────────────────────
def comparar_estrategias(n_escrituras: int = 20) -> dict:
    """
    Ejecuta las tres estrategias con el mismo conjunto de datos
    y compara latencias, disponibilidad y consistencia.
    """
    sincrona = ReplicacionSincrona(num_replicas=3)
    asincrona = ReplicacionAsincrona(num_replicas=3)
    multi = ReplicacionMultiLider(["region-a", "region-b"])

    resultados = {"sincrona": [], "asincrona": [], "multi_lider": []}

    for i in range(n_escrituras):
        dato = {"id": i, "valor": f"dato_{i}", "timestamp_unix": time.time()}

        r_s = sincrona.escribir(f"clave_{i}", dato)
        resultados["sincrona"].append(r_s.latencia_ms)

        r_a = asincrona.escribir(f"clave_{i}", dato)
        resultados["asincrona"].append(r_a.latencia_ms)

        r_m = multi.escribir_en_region("region-a", f"clave_{i}", dato)
        resultados["multi_lider"].append(r_m.latencia_ms)

    return {
        estrategia: {
            "latencia_promedio_ms": round(sum(lats) / len(lats), 2),
            "latencia_max_ms": round(max(lats), 2),
            "latencia_min_ms": round(min(lats), 2),
            "escrituras": len(lats),
        }
        for estrategia, lats in resultados.items()
    }


# ── Demo ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    print("=" * 65)
    print("  ESTRATEGIAS DE REPLICACIÓN DE DATOS")
    print("  (Teorema CAP: Consistencia vs Disponibilidad)")
    print("=" * 65)

    print("\n⏳ Comparando 15 escrituras por estrategia...")
    resultado = comparar_estrategias(n_escrituras=15)

    print("\n📊 Resultados comparativos:")
    print(f"{'Estrategia':<20} {'Latencia Prom':>15} {'Latencia Máx':>14} {'Latencia Mín':>14}")
    print("-" * 65)
    for nombre, stats in resultado.items():
        print(
            f"{nombre:<20} "
            f"{stats['latencia_promedio_ms']:>13.1f}ms "
            f"{stats['latencia_max_ms']:>12.1f}ms "
            f"{stats['latencia_min_ms']:>12.1f}ms"
        )

    print("\n💡 Conclusiones:")
    print("  Síncrona   → Mayor consistencia, mayor latencia (CP según CAP)")
    print("  Asíncrona  → Menor latencia, riesgo de pérdida (AP según CAP)")
    print("  Multi-líder → Máx. disponibilidad geográfica, requiere resolución conflictos")
