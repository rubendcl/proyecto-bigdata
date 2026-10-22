"""
plan_drp.py
===========
Implementación de un Plan de Recuperación ante Desastres (Disaster Recovery Plan).

Conceptos del BCP / DRP implementados:
1. Análisis de impacto (BIA): identifica procesos críticos y su tolerancia.
2. Sitios de respaldo: Warm / Hot / Cold standby.
3. Replicación continua hacia sitio de respaldo.
4. Failover automático y failback manual.
5. Métricas RTO y RPO.
6. Runbooks de recuperación documentados.
7. Simulacros periódicos.

Arquitectura multi-región implementada:
  Región A (Activa) → Región B (Standby) → Región C (Backup frío)
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from src.config import Configuracion

logger = logging.getLogger(__name__)
cfg = Configuracion()


# ── Enumeraciones ──────────────────────────────────────────────────────────────
class EstadoSistema(Enum):
    OPERATIVO = "OPERATIVO"
    DEGRADADO = "DEGRADADO"        # Funciona con capacidad reducida
    FAILOVER_ACTIVO = "FAILOVER"   # Operando desde sitio de respaldo
    RECUPERANDO = "RECUPERANDO"
    CAIDO = "CAIDO"


class TipoSitio(Enum):
    HOT = "hot"      # Sincronizado en tiempo real, failover < 1 min
    WARM = "warm"    # Semisincronizado, failover 5-15 min
    COLD = "cold"    # Backup periódico, failover > 1 hora


class PrioridadProceso(Enum):
    CRITICO = 1     # Debe restaurarse en minutos
    ALTO = 2        # Debe restaurarse en horas
    MEDIO = 3       # Puede esperar días
    BAJO = 4        # No urgente


# ── Modelos de datos ───────────────────────────────────────────────────────────
class ProcesoNegocio:
    """Representa un proceso de negocio con sus métricas de criticidad."""

    def __init__(
        self,
        nombre: str,
        prioridad: PrioridadProceso,
        rto_minutos: int,
        rpo_minutos: int,
        descripcion: str = "",
    ):
        self.nombre = nombre
        self.prioridad = prioridad
        self.rto_minutos = rto_minutos
        self.rpo_minutos = rpo_minutos
        self.descripcion = descripcion
        self.estado = EstadoSistema.OPERATIVO

    def to_dict(self) -> dict:
        return {
            "nombre": self.nombre,
            "prioridad": self.prioridad.name,
            "rto_minutos": self.rto_minutos,
            "rpo_minutos": self.rpo_minutos,
            "descripcion": self.descripcion,
            "estado": self.estado.value,
        }


class SitioRespaldo:
    """Representa un sitio de respaldo (Warm/Hot/Cold)."""

    def __init__(self, nombre: str, tipo: TipoSitio, region: str):
        self.nombre = nombre
        self.tipo = tipo
        self.region = region
        self.activo = False
        self.ultimo_sync = None
        self.datos_sincronizados: List[dict] = []
        self.metricas = {
            "sincronizaciones": 0,
            "registros_replicados": 0,
            "fallos_sync": 0,
        }

    def sincronizar(self, datos: List[dict]) -> bool:
        """Simula la replicación de datos al sitio de respaldo."""
        # Latencia simulada según tipo de sitio
        latencia = {"hot": 0.01, "warm": 0.5, "cold": 5.0}[self.tipo.value]
        time.sleep(latencia)

        self.datos_sincronizados.extend(datos)
        self.ultimo_sync = datetime.now(timezone.utc).isoformat()
        self.metricas["sincronizaciones"] += 1
        self.metricas["registros_replicados"] += len(datos)
        logger.debug(
            f"Sitio '{self.nombre}' ({self.tipo.value.upper()}): "
            f"{len(datos)} registros sincronizados."
        )
        return True

    def to_dict(self) -> dict:
        return {
            "nombre": self.nombre,
            "tipo": self.tipo.value,
            "region": self.region,
            "activo": self.activo,
            "ultimo_sync": self.ultimo_sync,
            "registros_en_sitio": len(self.datos_sincronizados),
            "metricas": self.metricas,
        }


# ── Plan DRP ───────────────────────────────────────────────────────────────────
class PlanDRP:
    """
    Implementación del Plan de Recuperación ante Desastres.

    Orquesta:
    - Replicación continua (RPO)
    - Detección de fallos y failover automático (RTO)
    - Failback manual al sitio primario
    - Runbooks de recuperación
    - Registro de incidentes y simulacros
    """

    def __init__(self):
        # ── Procesos críticos del negocio (Análisis de Impacto - BIA)
        self.procesos: List[ProcesoNegocio] = [
            ProcesoNegocio("Procesamiento de pagos", PrioridadProceso.CRITICO, rto_minutos=5, rpo_minutos=1),
            ProcesoNegocio("Detección de fraude", PrioridadProceso.CRITICO, rto_minutos=10, rpo_minutos=5),
            ProcesoNegocio("Pipeline de ingesta Kafka", PrioridadProceso.ALTO, rto_minutos=15, rpo_minutos=5),
            ProcesoNegocio("Data Warehouse queries", PrioridadProceso.ALTO, rto_minutos=30, rpo_minutos=15),
            ProcesoNegocio("Dashboard analítico", PrioridadProceso.MEDIO, rto_minutos=60, rpo_minutos=60),
            ProcesoNegocio("Reportes batch nocturnos", PrioridadProceso.BAJO, rto_minutos=480, rpo_minutos=1440),
        ]

        # ── Sitios de respaldo (Arquitectura multi-región)
        self.sitio_primario = SitioRespaldo(
            cfg.recuperacion.sitio_primario, TipoSitio.HOT, "region-a"
        )
        self.sitio_standby = SitioRespaldo(
            cfg.recuperacion.sitio_standby, TipoSitio.WARM, "region-b"
        )
        self.sitio_cold = SitioRespaldo(
            cfg.recuperacion.sitio_cold, TipoSitio.COLD, "region-c"
        )
        self.sitio_primario.activo = True

        # ── Estado del sistema
        self._estado = EstadoSistema.OPERATIVO
        self._sitio_activo = self.sitio_primario
        self._incidentes: List[dict] = []
        self._simulacros: List[dict] = []
        self._datos_primario: List[dict] = []

        # ── Hilo de replicación continua
        self._replicacion_activa = False
        self._hilo_replicacion: Optional[threading.Thread] = None

        # ── Métricas RTO / RPO
        self._timestamp_ultimo_backup = time.time()
        self._timestamp_inicio_fallo: Optional[float] = None
        self._timestamp_restauracion: Optional[float] = None

        logger.info("✅ Plan DRP iniciado. Sistema OPERATIVO en sitio primario.")

    # ── Replicación continua ───────────────────────────────────────────────────
    def iniciar_replicacion(self) -> None:
        """Inicia la replicación continua en segundo plano (RPO continuo)."""
        self._replicacion_activa = True

        def _ciclo_replicacion():
            logger.info("🔄 Replicación continua iniciada.")
            while self._replicacion_activa:
                # Replicar desde el último registro confirmado en standby.
                # Copiar solo los últimos 100 puede omitir registros antiguos
                # durante la sincronización inicial y reportarlos como perdidos.
                registros_confirmados = len(self.sitio_standby.datos_sincronizados)
                datos_nuevos = self._datos_primario[registros_confirmados:]
                if datos_nuevos:
                    self.sitio_standby.sincronizar(datos_nuevos)
                    self._timestamp_ultimo_backup = time.time()

                time.sleep(cfg.recuperacion.intervalo_backup_segundos / 10)  # Simulación acelerada

        self._hilo_replicacion = threading.Thread(
            target=_ciclo_replicacion, daemon=True, name="replicacion-drp"
        )
        self._hilo_replicacion.start()

    def detener_replicacion(self) -> None:
        self._replicacion_activa = False

    # ── Registro de datos ──────────────────────────────────────────────────────
    def registrar_dato(self, dato: dict) -> None:
        """Ingresa un dato al sistema primario."""
        self._datos_primario.append({**dato, "_timestamp_ingreso": time.time()})

    # ── Failover ───────────────────────────────────────────────────────────────
    def simular_fallo_primario(self, causa: str = "fallo_hardware") -> None:
        """Simula un desastre en el sitio primario y activa el DRP."""
        logger.critical(f"💥 DESASTRE en sitio primario: {causa.upper()}")
        self._timestamp_inicio_fallo = time.time()
        self._estado = EstadoSistema.CAIDO

        incidente = {
            "id": f"INC-{int(time.time())}",
            "causa": causa,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sitio_afectado": self._sitio_activo.nombre,
            "datos_en_riesgo": len(self._datos_primario),
        }
        self._incidentes.append(incidente)
        logger.critical(f"🚨 Incidente registrado: {incidente['id']}")

    def ejecutar_failover(self) -> Dict[str, Any]:
        """
        Runbook de Failover:
        1. Confirmar fallo del sitio primario
        2. Activar sitio de respaldo
        3. Redirigir tráfico (DNS/anycast)
        4. Validar integridad de datos
        5. Notificar stakeholders
        """
        if self._estado not in [EstadoSistema.CAIDO, EstadoSistema.DEGRADADO]:
            return {"estado": "failover_no_necesario"}

        logger.warning("🔄 Iniciando FAILOVER al sitio de respaldo...")
        inicio_failover = time.time()

        # Paso 1: Activar sitio standby
        self.sitio_standby.activo = True
        self.sitio_primario.activo = False
        self._sitio_activo = self.sitio_standby
        logger.info(f"✅ Paso 1: Sitio '{self.sitio_standby.nombre}' activado.")

        # Paso 2: Simular redireccionamiento de tráfico
        time.sleep(0.5)
        logger.info("✅ Paso 2: DNS/anycast redirigido al sitio de respaldo.")

        # Paso 3: Validar integridad de datos en standby
        registros_standby = len(self.sitio_standby.datos_sincronizados)
        registros_perdidos = max(0, len(self._datos_primario) - registros_standby)
        logger.info(
            f"✅ Paso 3: Integridad validada. "
            f"{registros_standby} registros disponibles, "
            f"{registros_perdidos} posiblemente perdidos (RPO)."
        )

        # Paso 4: Sistema operativo desde standby
        self._estado = EstadoSistema.FAILOVER_ACTIVO
        self._timestamp_restauracion = time.time()

        tiempo_failover = (self._timestamp_restauracion - self._timestamp_inicio_fallo) * 60
        rto_real = self._timestamp_restauracion - (self._timestamp_inicio_fallo or self._timestamp_restauracion)
        rpo_real = time.time() - self._timestamp_ultimo_backup

        resultado = {
            "estado_post_failover": self._estado.value,
            "sitio_activo": self._sitio_activo.nombre,
            "rto_real_minutos": round(rto_real / 60, 2),
            "rto_objetivo_minutos": cfg.recuperacion.rto_objetivo_minutos,
            "rto_cumplido": (rto_real / 60) <= cfg.recuperacion.rto_objetivo_minutos,
            "rpo_real_minutos": round(rpo_real / 60, 2),
            "rpo_objetivo_minutos": cfg.recuperacion.rpo_objetivo_minutos,
            "rpo_cumplido": (rpo_real / 60) <= cfg.recuperacion.rpo_objetivo_minutos,
            "registros_recuperados": registros_standby,
            "registros_perdidos": registros_perdidos,
            "duracion_failover_s": round(time.time() - inicio_failover, 2),
        }

        logger.info(
            f"{'✅' if resultado['rto_cumplido'] else '❌'} RTO: "
            f"{resultado['rto_real_minutos']:.2f} min "
            f"(objetivo: {resultado['rto_objetivo_minutos']} min)"
        )
        logger.info(
            f"{'✅' if resultado['rpo_cumplido'] else '❌'} RPO: "
            f"{resultado['rpo_real_minutos']:.2f} min "
            f"(objetivo: {resultado['rpo_objetivo_minutos']} min)"
        )
        return resultado

    # ── Simulacros ─────────────────────────────────────────────────────────────
    def ejecutar_simulacro(self, tipo: str = "failover_completo") -> dict:
        """
        Ejecuta un simulacro del plan de recuperación.
        Los simulacros validan RTO y RPO reales antes de un desastre real.
        """
        logger.info(f"🧪 SIMULACRO INICIADO: tipo='{tipo}'")
        inicio = time.time()

        # Respaldar estado actual
        estado_previo = self._estado
        sitio_previo = self._sitio_activo

        # Simular el escenario
        self.simular_fallo_primario(causa=f"simulacro_{tipo}")
        resultado_failover = self.ejecutar_failover()

        # Restaurar estado post-simulacro
        self._estado = estado_previo
        self._sitio_activo = sitio_previo
        self.sitio_primario.activo = True
        self.sitio_standby.activo = False

        resultado_simulacro = {
            "tipo_simulacro": tipo,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duracion_total_s": round(time.time() - inicio, 2),
            "resultado_failover": resultado_failover,
            "brechas_detectadas": self._detectar_brechas(resultado_failover),
            "recomendaciones": self._generar_recomendaciones(resultado_failover),
        }

        self._simulacros.append(resultado_simulacro)
        logger.info(
            f"✅ Simulacro completado en {resultado_simulacro['duracion_total_s']}s. "
            f"Brechas: {len(resultado_simulacro['brechas_detectadas'])}"
        )
        return resultado_simulacro

    def _detectar_brechas(self, resultado: dict) -> List[str]:
        brechas = []
        if not resultado.get("rto_cumplido"):
            brechas.append(
                f"RTO excedido: {resultado['rto_real_minutos']:.1f} min > "
                f"{resultado['rto_objetivo_minutos']} min objetivo"
            )
        if not resultado.get("rpo_cumplido"):
            brechas.append(
                f"RPO excedido: {resultado['rpo_real_minutos']:.1f} min > "
                f"{resultado['rpo_objetivo_minutos']} min objetivo"
            )
        if resultado.get("registros_perdidos", 0) > 0:
            brechas.append(f"{resultado['registros_perdidos']} registros sin replicar en standby")
        return brechas

    def _generar_recomendaciones(self, resultado: dict) -> List[str]:
        recomendaciones = []
        if not resultado.get("rto_cumplido"):
            recomendaciones.append("Considerar sitio HOT en lugar de WARM para reducir RTO.")
        if not resultado.get("rpo_cumplido"):
            recomendaciones.append("Reducir intervalo de replicación para mejorar RPO.")
        if not recomendaciones:
            recomendaciones.append("Plan DRP cumple con los objetivos RTO/RPO. ✅")
        return recomendaciones

    # ── Reporte ────────────────────────────────────────────────────────────────
    def generar_reporte(self) -> dict:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "estado_sistema": self._estado.value,
            "sitio_activo": self._sitio_activo.nombre,
            "procesos_criticos": [p.to_dict() for p in self.procesos if p.prioridad == PrioridadProceso.CRITICO],
            "sitios": {
                "primario": self.sitio_primario.to_dict(),
                "standby": self.sitio_standby.to_dict(),
                "cold": self.sitio_cold.to_dict(),
            },
            "incidentes_totales": len(self._incidentes),
            "simulacros_realizados": len(self._simulacros),
            "ultimo_backup_hace_s": round(time.time() - self._timestamp_ultimo_backup, 1),
        }


# ── Demo ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import sys
    sys.path.insert(0, ".")
    from src.ingesta.simulador_transacciones import generar_lote

    print("=" * 65)
    print("  PLAN DE RECUPERACIÓN ANTE DESASTRES (DRP)")
    print("=" * 65)

    drp = PlanDRP()

    # Cargar datos al sistema primario
    transacciones = generar_lote(200)
    for tx in transacciones:
        drp.registrar_dato(tx)

    # Iniciar replicación
    drp.iniciar_replicacion()
    time.sleep(2)  # Dejar que la replicación avance

    # Ejecutar simulacro
    print("\n🧪 Ejecutando simulacro de recuperación...")
    resultado = drp.ejecutar_simulacro("fallo_datacenter")

    print("\n📊 Resultado del Simulacro:")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))

    # Reporte general
    print("\n📋 Estado del Sistema DRP:")
    reporte = drp.generar_reporte()
    print(json.dumps(reporte, indent=2, ensure_ascii=False))

    drp.detener_replicacion()
