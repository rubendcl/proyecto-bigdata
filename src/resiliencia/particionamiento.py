"""
particionamiento.py
===================
Implementación de estrategias de particionamiento de datos (sharding).

Particionamiento horizontal (Row-based sharding):
  Las filas se distribuyen entre distintos nodos según una clave de partición.
  → Ejemplo: transacciones divididas por región o por hash del cliente_id.

Particionamiento vertical (Column-based sharding):
  Las columnas se agrupan y separan según su patrón de acceso.
  → Ejemplo: datos de perfil vs. datos de actividad en almacenes distintos.

El particionamiento funcional divide el dominio de datos en unidades manejables,
permitiendo escalar cada partición de forma independiente (scale-out).
"""

import hashlib
import logging
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Estrategias de particionamiento horizontal ─────────────────────────────────

class ShardHashing:
    """
    Particionamiento por hash consistente (Consistent Hashing).
    Distribuye la carga uniformemente y minimiza la redistribución
    cuando se agregan o eliminan shards (elasticidad).
    """

    def __init__(self, num_shards: int = 3, replica_factor: int = 100):
        self.num_shards = num_shards
        self.replica_factor = replica_factor  # Nodos virtuales para uniformidad
        self._anillo: List[Tuple[int, str]] = []
        self._construir_anillo()
        logger.info(f"Shard Hashing iniciado: {num_shards} shards, {replica_factor} réplicas virtuales")

    def _construir_anillo(self) -> None:
        """Construye el anillo de hash consistente con nodos virtuales."""
        for i in range(self.num_shards):
            for j in range(self.replica_factor):
                clave_virtual = f"shard_{i}_virtual_{j}"
                hash_pos = int(hashlib.md5(clave_virtual.encode()).hexdigest(), 16)
                self._anillo.append((hash_pos, f"shard_{i}"))
        self._anillo.sort(key=lambda x: x[0])

    def asignar_shard(self, clave: str) -> str:
        """
        Determina a qué shard pertenece una clave.
        Recorre el anillo y encuentra el primer nodo ≥ hash(clave).
        """
        if not self._anillo:
            return "shard_0"
        hash_clave = int(hashlib.md5(clave.encode()).hexdigest(), 16)
        for pos, shard in self._anillo:
            if hash_clave <= pos:
                return shard
        # Wrap-around: la clave está más allá del último nodo
        return self._anillo[0][1]

    def distribucion(self, claves: List[str]) -> Dict[str, int]:
        """Analiza la distribución de claves entre shards."""
        conteo: Dict[str, int] = defaultdict(int)
        for clave in claves:
            shard = self.asignar_shard(clave)
            conteo[shard] += 1
        return dict(sorted(conteo.items()))


class ShardPorRango:
    """
    Particionamiento por rango.
    Cada shard gestiona un rango específico de la clave.
    Útil cuando las consultas suelen ser rangos (ej: fechas, IDs secuenciales).

    Desventaja: puede generar hotspots si los rangos no están balanceados.
    """

    def __init__(self, rangos: List[Tuple[Any, Any, str]]):
        """
        rangos: [(min_inclusivo, max_exclusivo, nombre_shard), ...]
        Ejemplo: [(0, 100, "shard_0"), (100, 200, "shard_1"), (200, None, "shard_2")]
        """
        self.rangos = rangos
        logger.info(f"Shard por Rango iniciado: {len(rangos)} rangos")

    def asignar_shard(self, valor: Any) -> Optional[str]:
        for min_val, max_val, shard in self.rangos:
            if min_val <= valor and (max_val is None or valor < max_val):
                return shard
        return None


class ShardGeografico:
    """
    Particionamiento geográfico (Geographic Sharding).
    Mejora la latencia al colocar datos cerca del usuario.
    Facilita el cumplimiento normativo por jurisdicción (ej: GDPR, regulaciones Bolivia).
    """

    MAPA_REGION_SHARD = {
        "norte":  "shard_0",
        "sur":    "shard_0",
        "este":   "shard_1",
        "oeste":  "shard_1",
        "centro": "shard_2",
    }

    def asignar_shard(self, region: str) -> str:
        return self.MAPA_REGION_SHARD.get(region.lower(), "shard_2")


# ── Gestor de particionamiento ─────────────────────────────────────────────────

class GestorParticionamiento:
    """
    Gestor centralizado que combina múltiples estrategias de particionamiento.
    Implementa particionamiento horizontal Y vertical sobre el mismo dataset.
    """

    # Definición de particionamiento vertical:
    # Agrupa columnas por patrón de acceso (hot vs cold data)
    COLUMNAS_PERFIL = {
        "cliente_id", "banco_emisor", "region",
    }
    COLUMNAS_TRANSACCION = {
        "id_transaccion", "timestamp_utc", "tipo",
        "monto", "moneda", "canal",
    }
    COLUMNAS_COMERCIO = {
        "comercio_id", "comercio_nombre", "comercio_categoria",
    }
    COLUMNAS_RIESGO = {
        "es_fraude_potencial", "score_riesgo", "metadata",
    }

    def __init__(self, num_shards: int = 3, estrategia: str = "hash"):
        self.num_shards = num_shards
        self.estrategia = estrategia
        self._shards: Dict[str, List[dict]] = {
            f"shard_{i}": [] for i in range(num_shards)
        }
        # Almacenes verticales
        self._almacen_perfil: List[dict] = []
        self._almacen_transacciones: List[dict] = []
        self._almacen_comercio: List[dict] = []
        self._almacen_riesgo: List[dict] = []

        if estrategia == "hash":
            self._shard_fn = ShardHashing(num_shards)
        elif estrategia == "geografica":
            self._shard_fn = ShardGeografico()
        else:
            raise ValueError(f"Estrategia desconocida: {estrategia}")

        self._metricas = {"inserciones": 0, "consultas": 0}
        logger.info(f"Gestor de Particionamiento iniciado (estrategia='{estrategia}', shards={num_shards})")

    # ── Particionamiento Horizontal ────────────────────────────────────────────
    def insertar(self, registro: dict) -> str:
        """Inserta un registro en el shard correspondiente."""
        if self.estrategia == "geografica":
            shard_id = self._shard_fn.asignar_shard(registro.get("region", "centro"))
        else:
            # Usar una clave de alta cardinalidad. `shard_key` ya viene precalculada
            # como shard_0..shard_N y volver a hashearla puede dejar shards vacíos.
            clave = registro.get("cliente_id") or registro.get("shard_key", "default")
            shard_id = self._shard_fn.asignar_shard(clave)

        self._shards[shard_id].append(registro)
        self._metricas["inserciones"] += 1
        logger.debug(f"Registro {registro.get('id_transaccion', '')[:8]}... → {shard_id}")
        return shard_id

    def insertar_lote(self, registros: List[dict]) -> Dict[str, int]:
        """Inserta un lote de registros distribuyéndolos entre shards."""
        distribucion: Dict[str, int] = defaultdict(int)
        t0 = time.perf_counter()
        for r in registros:
            shard_id = self.insertar(r)
            distribucion[shard_id] += 1
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            f"Lote de {len(registros)} insertado en {elapsed_ms:.1f}ms. "
            f"Distribución: {dict(distribucion)}"
        )
        return dict(distribucion)

    def consultar_shard(self, shard_id: str) -> List[dict]:
        """Recupera todos los registros de un shard específico."""
        self._metricas["consultas"] += 1
        return self._shards.get(shard_id, [])

    # ── Particionamiento Vertical ──────────────────────────────────────────────
    def particionar_verticalmente(self, registro: dict) -> Dict[str, dict]:
        """
        Separa un registro en sub-registros por grupo de columnas.
        Columnas de acceso frecuente (hot) se almacenan separado de las frías (cold),
        optimizando lecturas y reduciendo I/O.
        """
        clave_pk = {
            "id_transaccion": registro.get("id_transaccion"),
            "timestamp_unix": registro.get("timestamp_unix"),
        }

        particiones = {
            "perfil":       {**clave_pk, **{k: registro.get(k) for k in self.COLUMNAS_PERFIL}},
            "transaccion":  {**clave_pk, **{k: registro.get(k) for k in self.COLUMNAS_TRANSACCION}},
            "comercio":     {**clave_pk, **{k: registro.get(k) for k in self.COLUMNAS_COMERCIO}},
            "riesgo":       {**clave_pk, **{k: registro.get(k) for k in self.COLUMNAS_RIESGO}},
        }

        # Almacenar en sus respectivos "almacenes"
        self._almacen_perfil.append(particiones["perfil"])
        self._almacen_transacciones.append(particiones["transaccion"])
        self._almacen_comercio.append(particiones["comercio"])
        self._almacen_riesgo.append(particiones["riesgo"])

        return particiones

    # ── Estadísticas ───────────────────────────────────────────────────────────
    def estadisticas(self) -> dict:
        """Retorna estadísticas de distribución para detectar hotspots."""
        total = sum(len(v) for v in self._shards.values())
        distribución = {k: len(v) for k, v in self._shards.items()}
        valores = list(distribución.values())
        balance = min(valores) / max(valores) * 100 if max(valores) > 0 else 100.0

        return {
            "estrategia": self.estrategia,
            "num_shards": self.num_shards,
            "total_registros": total,
            "distribucion": distribución,
            "balance_pct": round(balance, 1),  # 100% = distribución perfecta
            "hotspot_detectado": balance < 70,  # < 70% balance = posible hotspot
            "metricas": self._metricas,
        }

    def __repr__(self) -> str:
        return (
            f"GestorParticionamiento(estrategia='{self.estrategia}', "
            f"shards={self.num_shards}, registros={sum(len(v) for v in self._shards.values())})"
        )


# ── Demo ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    import sys
    sys.path.insert(0, ".")
    from src.ingesta.simulador_transacciones import generar_lote

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    print("=" * 65)
    print("  DEMOSTRACIÓN DE PARTICIONAMIENTO DE DATOS")
    print("=" * 65)

    # ── 1. Particionamiento Horizontal por Hash ────────────────────────────────
    print("\n1️⃣  Particionamiento Horizontal (Hash Consistente):")
    gestor_hash = GestorParticionamiento(num_shards=3, estrategia="hash")
    transacciones = generar_lote(500)
    gestor_hash.insertar_lote(transacciones)
    stats = gestor_hash.estadisticas()
    print(f"   Total: {stats['total_registros']} registros")
    for shard, conteo in stats["distribucion"].items():
        barra = "█" * (conteo // 10)
        print(f"   {shard}: {barra} {conteo}")
    print(f"   Balance de distribución: {stats['balance_pct']}%")
    print(f"   Hotspot detectado: {'⚠️ SÍ' if stats['hotspot_detectado'] else '✅ NO'}")

    # ── 2. Particionamiento Horizontal Geográfico ──────────────────────────────
    print("\n2️⃣  Particionamiento Geográfico:")
    gestor_geo = GestorParticionamiento(num_shards=3, estrategia="geografica")
    gestor_geo.insertar_lote(transacciones)
    stats_geo = gestor_geo.estadisticas()
    for shard, conteo in stats_geo["distribucion"].items():
        print(f"   {shard}: {conteo} registros")

    # ── 3. Particionamiento Vertical ───────────────────────────────────────────
    print("\n3️⃣  Particionamiento Vertical (separación de columnas):")
    tx_ejemplo = transacciones[0]
    particiones = gestor_hash.particionar_verticalmente(tx_ejemplo)
    for nombre, datos in particiones.items():
        cols = list(datos.keys())
        print(f"   Partición '{nombre}': {cols}")

    print("\n✅ Demostración de particionamiento completada.")
