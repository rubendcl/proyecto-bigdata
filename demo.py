"""
demo.py
=======
Demostración integral standalone — no requiere Docker ni servicios externos.
Ejecuta los cinco bloques del módulo académico con datos simulados localmente.

Uso:
    python demo.py               # Ejecuta todos los bloques
    python demo.py --bloque 1    # Ejecuta solo el Bloque 1
    python demo.py --bloque 3    # Ejecuta solo el Bloque 3
    python demo.py --completo    # Modo verbose con todas las métricas

Bloques disponibles:
    1 → Soluciones Escalables (ETL, Lakehouse)
    2 → Selección de Tecnologías (Batch vs Streaming)
    3 → Alta Disponibilidad (Circuit Breaker, Tolerancia a Fallos)
    4 → Recuperación ante Desastres (DRP, RTO/RPO)
    5 → Replicación y Particionamiento (CAP, Sharding)
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Asegurar que el paquete src sea importable desde cualquier directorio
sys.path.insert(0, str(Path(__file__).parent))

# Configuración de logging con formato limpio
logging.basicConfig(
    level=logging.WARNING,   # Silenciar logs internos para demo limpia
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ── Utilidades de presentación ─────────────────────────────────────────────────
def encabezado(titulo: str, subtitulo: str = ""):
    ancho = 65
    print("\n" + "═" * ancho)
    print(f"  {titulo}")
    if subtitulo:
        print(f"  {subtitulo}")
    print("═" * ancho)

def seccion(texto: str):
    print(f"\n  ─── {texto} {'─' * max(0, 55 - len(texto))}")

def resultado(clave: str, valor, ok: bool = True):
    icono = "✅" if ok else "❌"
    print(f"  {icono} {clave:<38} {valor}")

def separador():
    print()


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 1 — SOLUCIONES ESCALABLES
# ══════════════════════════════════════════════════════════════════════════════
def demo_bloque1():
    encabezado(
        "BLOQUE 1 · SOLUCIONES ESCALABLES",
        "Arquitecturas Lambda / Kappa · ETL Batch · Lakehouse Bronze→Silver→Gold"
    )
    from src.ingesta.simulador_transacciones import generar_lote, generar_transaccion
    from src.procesamiento.motor_batch import MotorBatch

    # ── Las 5 V's del Big Data ─────────────────────────────────────────────────
    seccion("Las 5 Dimensiones del Big Data (una transacción real)")
    tx = generar_transaccion()
    resultado("Volumen (dato generado)", f"ID: {tx['id_transaccion'][:12]}...")
    resultado("Velocidad (timestamp)",   tx["timestamp_utc"])
    resultado("Variedad (tipo)",         f"{tx['tipo']} | {tx['moneda']} | {tx['canal']}")
    resultado("Veracidad (score riesgo)",f"{tx['score_riesgo']} | Fraude: {tx['es_fraude_potencial']}")
    resultado("Valor (monto USD aprox)", f"${tx['monto']:.2f} {tx['moneda']}")

    # ── Principios escalables: escalado horizontal ─────────────────────────────
    seccion("Escalado Horizontal — Generación de carga")
    for n in [100, 500, 1000]:
        t0 = time.perf_counter()
        lote = generar_lote(n)
        ms = (time.perf_counter() - t0) * 1000
        tps = n / (ms / 1000)
        resultado(f"Lote de {n:>5} transacciones", f"{ms:>7.1f}ms | {tps:>8,.0f} TPS")

    # ── Pipeline ETL: Bronze → Silver → Gold ──────────────────────────────────
    seccion("Pipeline ETL — Arquitectura Lambda (capa Batch)")
    motor = MotorBatch(usar_spark=False)
    lote_etl = generar_lote(300)
    r = motor.ejecutar_pipeline_etl(lote_etl)

    resultado("Registros entrada",         f"{r['registros_entrada']:,}")
    resultado("Duración pipeline",         f"{r['duracion_s']:.3f}s")
    resultado("Throughput",                f"{r['throughput_registros_s']:,.0f} registros/s")
    kpis = r.get("kpis", {})
    resultado("Total transacciones (Gold)",f"{kpis.get('total_transacciones', 0):,}")
    resultado("Monto total USD (Gold)",    f"${kpis.get('monto_total_usd', 0):>12,.2f}")
    resultado("Tasa de fraude",            f"{kpis.get('tasa_fraude_pct', 0):.3f}%")
    resultado("Capas escritas",            "Bronze ✓  Silver ✓  Gold ✓")


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 2 — SELECCIÓN DE TECNOLOGÍAS
# ══════════════════════════════════════════════════════════════════════════════
def demo_bloque2():
    encabezado(
        "BLOQUE 2 · SELECCIÓN DE TECNOLOGÍAS",
        "Batch vs. Streaming · Data Lake/Warehouse/Lakehouse · Criterios TCO"
    )
    from src.ingesta.simulador_transacciones import generar_lote, flujo_continuo

    # ── Batch vs Streaming: latencias comparadas ───────────────────────────────
    seccion("Batch vs Streaming — Comparación de latencia")

    # Batch: espera a acumular datos
    lote = generar_lote(1000)
    t0 = time.perf_counter()
    procesados = sum(1 for _ in lote)  # Simula procesamiento batch
    lat_batch = (time.perf_counter() - t0) * 1000

    print(f"\n  {'Motor':<22} {'Modelo':<12} {'Latencia':<15} {'Throughput':<15} {'Caso de uso'}")
    print(f"  {'─'*22} {'─'*12} {'─'*15} {'─'*15} {'─'*20}")

    # Batch
    tps_batch = procesados / (lat_batch / 1000)
    print(f"  {'Apache Spark':<22} {'Batch':<12} {lat_batch:>8.1f}ms    "
          f"{tps_batch:>10,.0f} r/s    ETL nocturno, reportes")

    # Streaming (simula eventos uno a uno)
    gen = flujo_continuo(intervalo_segundos=0)
    lats_stream = []
    for _ in range(50):
        t0 = time.perf_counter()
        _ = next(gen)
        lats_stream.append((time.perf_counter() - t0) * 1000)
    lat_stream_p99 = sorted(lats_stream)[int(len(lats_stream) * 0.99)]
    print(f"  {'Apache Kafka/Flink':<22} {'Streaming':<12} {lat_stream_p99:>8.2f}ms    "
          f"{'tiempo real':>10}       Fraude, IoT, alertas")

    # ── Capas de almacenamiento ────────────────────────────────────────────────
    seccion("Capas de Almacenamiento — Características")
    capas = [
        ("Data Lake",      "Zona-raw (bronze)",    "Cualquier formato", "Bajo",   "Alta",     "Pantano de datos"),
        ("Data Warehouse", "PostgreSQL (gold)",     "SQL estructurado",  "Alto",   "Media",    "Costoso a escala"),
        ("Data Lakehouse", "Delta/Iceberg (gold)",  "Formatos abiertos", "Medio",  "Muy alta", "ACID sobre object storage"),
    ]
    print(f"\n  {'Tipo':<18} {'Implementación':<22} {'Formato':<20} {'Costo':<8} {'Flexib.':<10} {'Trade-off'}")
    print(f"  {'─'*18} {'─'*22} {'─'*20} {'─'*8} {'─'*10} {'─'*25}")
    for c in capas:
        print(f"  {c[0]:<18} {c[1]:<22} {c[2]:<20} {c[3]:<8} {c[4]:<10} {c[5]}")

    # ── Criterios de selección tecnológica ────────────────────────────────────
    seccion("Criterios de Selección Tecnológica (TCO)")
    criterios = [
        ("Rendimiento y latencia", "Throughput sostenido frente al caso de uso real"),
        ("Costo total (TCO)",      "Licenciamiento + infraestructura + curva de aprendizaje"),
        ("Madurez/interop.",       "Ecosistema, estándares abiertos, compatibilidad"),
        ("Seguridad y cumplimiento","Cifrado, acceso, GDPR / regulaciones Bolivia"),
        ("Escalabilidad",          "Capacidad de crecer + soporte de la comunidad"),
    ]
    for crit, desc in criterios:
        print(f"  ▸ {crit:<28} {desc}")


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 3 — ALTA DISPONIBILIDAD
# ══════════════════════════════════════════════════════════════════════════════
def demo_bloque3():
    encabezado(
        "BLOQUE 3 · ALTA DISPONIBILIDAD",
        "Circuit Breaker · Tolerancia a Fallos · Observabilidad · Patrones HA"
    )
    import random
    from src.resiliencia.circuit_breaker import CircuitBreaker, CircuitoBiertoError
    from src.monitoreo.observabilidad import SistemaObservabilidad

    # ── Circuit Breaker en acción ──────────────────────────────────────────────
    seccion("Circuit Breaker — Patrón de Resiliencia")
    cb = CircuitBreaker("servicio-pagos-demo", umbral_fallos=3, timeout_s=2)

    def servicio_inestable(prob_fallo: float = 0.7):
        if random.random() < prob_fallo:
            raise ConnectionError("Servicio no disponible")
        return {"latencia_ms": random.randint(20, 100)}

    print(f"\n  {'#':<5} {'Estado CB':<16} {'Resultado':<30} {'Acción'}")
    print(f"  {'─'*5} {'─'*16} {'─'*30} {'─'*25}")

    for i in range(1, 13):
        try:
            res = cb.ejecutar(servicio_inestable, prob_fallo=0.65)
            estado = cb.estado.value
            print(f"  [{i:02d}]  🟢 {estado:<13}  ✅ Éxito ({res['latencia_ms']}ms){'':15} Llamada procesada")
        except CircuitoBiertoError:
            print(f"  [{i:02d}]  🔴 {cb.estado.value:<13}  🚫 RECHAZADO (sin llamar){'':8} Fallo en cascada evitado")
        except ConnectionError:
            print(f"  [{i:02d}]  🟡 {cb.estado.value:<13}  ❌ Error de conexión{'':14} Fallo registrado")

        if i == 7:
            print(f"\n  ⏳ Esperando timeout del circuito (2s)...")
            time.sleep(2.1)

    separador()
    m = cb.obtener_metricas()
    resultado("Total llamadas",          m["total_llamadas"])
    resultado("Llamadas exitosas",       m["total_exitos"])
    resultado("Llamadas fallidas",       m["total_fallos"])
    resultado("Rechazos (CB abierto)",   m["total_rechazos"],    ok=m["total_rechazos"] > 0)
    resultado("Tasa de éxito",          f"{m['tasa_exito_pct']:.1f}%")

    # ── Patrones HA ────────────────────────────────────────────────────────────
    seccion("Patrones de Resiliencia en Arquitecturas Distribuidas")
    patrones = [
        ("Circuit Breaker",    "Evita fallos en cascada deteniendo llamadas a servicios degradados"),
        ("Failover Automático","Redirige tráfico a componente sano sin intervención manual"),
        ("Multi-AZ/Región",   "Distribuye carga entre zonas de disponibilidad independientes"),
        ("Checkpointing",      "Puntos de control periódicos para reanudar sin perder progreso"),
        ("Self-healing",       "Detección automática de fallos y reinicio de componentes"),
    ]
    for patron, desc in patrones:
        print(f"  ▸ {patron:<22} {desc}")

    # ── Métricas HA ────────────────────────────────────────────────────────────
    seccion("Métricas de Alta Disponibilidad")
    resultado("Objetivo disponibilidad", "99.99% ('4 nueves')")
    resultado("Tiempo de caída anual",   "≈ 52 minutos")
    resultado("Puntos únicos de falla",  "0 (tolerado en diseño HA)")
    resultado("Redundancia mínima",      "N+1 por componente crítico")

    # ── Observabilidad ─────────────────────────────────────────────────────────
    seccion("Observabilidad — Los 4 Pilares")
    obs = SistemaObservabilidad("demo-ha")
    for _ in range(50):
        traza = obs.iniciar_traza("procesar_pago")
        traza.agregar_span("kafka_consume",    random.uniform(2, 20))
        traza.agregar_span("validar",          random.uniform(1, 5))
        traza.agregar_span("detectar_fraude",  random.uniform(15, 60))
        obs.finalizar_traza(traza, error="Timeout" if random.random() < 0.04 else None)
        obs.mensajes_kafka_procesados.incrementar()

    snap = obs.snapshot()
    lat = snap.get("latencia", {})
    resultado("Pilar 1 — Métricas",     f"{snap['contadores']['mensajes_kafka']} mensajes procesados")
    resultado("Pilar 2 — Logs",         f"{len(snap.get('logs_recientes', []))} eventos registrados")
    resultado("Pilar 3 — Trazas",       f"P50={lat.get('p50',0):.0f}ms | P99={lat.get('p99',0):.0f}ms")
    resultado("Pilar 4 — Alertas",      f"Tasa error: {snap['tasa_error_pct']:.2f}%")


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 4 — RECUPERACIÓN ANTE DESASTRES
# ══════════════════════════════════════════════════════════════════════════════
def demo_bloque4():
    encabezado(
        "BLOQUE 4 · RECUPERACIÓN ANTE DESASTRES",
        "DRP · BCP · RTO/RPO · Multi-Región · Sitios Hot/Warm/Cold"
    )
    from src.recuperacion.plan_drp import PlanDRP
    from src.ingesta.simulador_transacciones import generar_lote

    drp = PlanDRP()

    # Cargar datos
    txs = generar_lote(150)
    for tx in txs:
        drp.registrar_dato(tx)

    drp.iniciar_replicacion()
    time.sleep(1.5)  # Permitir sincronización

    # ── Procesos críticos (BIA) ────────────────────────────────────────────────
    seccion("Análisis de Impacto de Negocio (BIA) — Procesos Críticos")
    print(f"\n  {'Proceso':<32} {'Prioridad':<12} {'RTO':<10} {'RPO'}")
    print(f"  {'─'*32} {'─'*12} {'─'*10} {'─'*10}")
    for p in drp.procesos:
        print(f"  {p.nombre:<32} {p.prioridad.name:<12} {p.rto_minutos:>5} min   {p.rpo_minutos:>5} min")

    # ── Sitios de respaldo ─────────────────────────────────────────────────────
    seccion("Arquitectura Multi-Región — Sitios de Respaldo")
    sitios = [
        (drp.sitio_primario, "🟢 ACTIVO"),
        (drp.sitio_standby,  "🟡 STANDBY"),
        (drp.sitio_cold,     "🔵 COLD"),
    ]
    for sitio, estado in sitios:
        print(f"  {estado}  {sitio.nombre:<30} Tipo: {sitio.tipo.value.upper():<6} Región: {sitio.region}")

    # ── Simulacro DRP ──────────────────────────────────────────────────────────
    seccion("Simulacro de Recuperación ante Desastres")
    print("  Ejecutando simulacro: fallo del datacenter primario...")
    resultado_sim = drp.ejecutar_simulacro("fallo_datacenter_principal")
    fo = resultado_sim.get("resultado_failover", {})

    separador()
    rto_ok = fo.get("rto_cumplido", False)
    rpo_ok = fo.get("rpo_cumplido", False)

    resultado("RTO real",                f"{fo.get('rto_real_minutos',0):.2f} min  (objetivo: {fo.get('rto_objetivo_minutos',15)} min)",  rto_ok)
    resultado("RTO cumplido",            "✅ SÍ" if rto_ok else "❌ NO",  rto_ok)
    resultado("RPO real",                f"{fo.get('rpo_real_minutos',0):.2f} min  (objetivo: {fo.get('rpo_objetivo_minutos',5)} min)",   rpo_ok)
    resultado("RPO cumplido",            "✅ SÍ" if rpo_ok else "❌ NO",  rpo_ok)
    resultado("Registros recuperados",   f"{fo.get('registros_recuperados',0):,}")
    resultado("Registros perdidos",      f"{fo.get('registros_perdidos',0):,}", ok=fo.get('registros_perdidos',0)==0)
    resultado("Duración failover",       f"{fo.get('duracion_failover_s',0):.2f}s")
    resultado("Sitio activo post-fallo", fo.get("sitio_activo", "-"))

    brechas = resultado_sim.get("brechas_detectadas", [])
    recomendaciones = resultado_sim.get("recomendaciones", [])
    if brechas:
        print(f"\n  ⚠️  Brechas detectadas ({len(brechas)}):")
        for b in brechas:
            print(f"     • {b}")
    if recomendaciones:
        print(f"\n  💡 Recomendaciones ({len(recomendaciones)}):")
        for r in recomendaciones:
            print(f"     • {r}")

    drp.detener_replicacion()


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 5 — REPLICACIÓN Y PARTICIONAMIENTO
# ══════════════════════════════════════════════════════════════════════════════
def demo_bloque5():
    encabezado(
        "BLOQUE 5 · REPLICACIÓN Y PARTICIONAMIENTO",
        "Sharding · Hashing Consistente · CAP · Estrategias de Replicación"
    )
    from src.resiliencia.particionamiento import GestorParticionamiento
    from src.resiliencia.replicacion import comparar_estrategias
    from src.ingesta.simulador_transacciones import generar_lote

    # ── Particionamiento Horizontal ────────────────────────────────────────────
    seccion("Particionamiento Horizontal (Sharding) — 3 Estrategias")
    transacciones = generar_lote(600)

    for estrategia in ["hash", "geografica"]:
        gestor = GestorParticionamiento(num_shards=3, estrategia=estrategia)
        t0 = time.perf_counter()
        gestor.insertar_lote(transacciones)
        ms = (time.perf_counter() - t0) * 1000
        stats = gestor.estadisticas()

        print(f"\n  Estrategia: {estrategia.upper()}")
        print(f"  {'Shard':<12} {'Registros':>12} {'Porcentaje':>12} {'Distribución'}")
        print(f"  {'─'*12} {'─'*12} {'─'*12} {'─'*30}")
        for shard, conteo in stats["distribucion"].items():
            pct = conteo / stats["total_registros"] * 100
            barra = "█" * int(pct / 3)
            print(f"  {shard:<12} {conteo:>12,} {pct:>11.1f}%  {barra}")
        print(f"  Balance: {stats['balance_pct']}% | Hotspot: {'⚠️ SÍ' if stats['hotspot_detectado'] else '✅ NO'} | {ms:.0f}ms")

    # ── Particionamiento Vertical ──────────────────────────────────────────────
    seccion("Particionamiento Vertical — Separación por patrón de acceso")
    gestor_h = GestorParticionamiento(num_shards=3, estrategia="hash")
    gestor_h.insertar_lote(transacciones[:1])
    particiones_v = gestor_h.particionar_verticalmente(transacciones[0])
    for nombre, cols in particiones_v.items():
        columnas = [k for k in cols.keys() if not k.startswith("_") and k != "id_transaccion"]
        print(f"  ▸ Partición '{nombre}':\t{', '.join(columnas)}")

    # ── Estrategias de Replicación ────────────────────────────────────────────
    seccion("Estrategias de Replicación — Comparación de latencias")
    print("  Ejecutando 15 escrituras por estrategia...")
    comparacion = comparar_estrategias(n_escrituras=15)

    print(f"\n  {'Estrategia':<22} {'Lat. Promedio':>14} {'Lat. Máx':>12} {'Consistencia':<14} {'Disponib.'}")
    print(f"  {'─'*22} {'─'*14} {'─'*12} {'─'*14} {'─'*10}")
    metas = {
        "sincrona":    ("Fuerte (CP)",  "Media"),
        "asincrona":   ("Eventual (AP)", "Alta"),
        "multi_lider": ("Eventual (AP)", "Muy Alta"),
    }
    for nombre, stats in comparacion.items():
        cons, disp = metas.get(nombre, ("-", "-"))
        print(
            f"  {nombre:<22} "
            f"{stats['latencia_promedio_ms']:>12.1f}ms "
            f"{stats['latencia_max_ms']:>10.1f}ms  "
            f"{cons:<16} {disp}"
        )

    # ── Teorema CAP ────────────────────────────────────────────────────────────
    seccion("Teorema CAP — Compromiso en sistemas distribuidos")
    print()
    print("         Consistencia (C)")
    print("              △")
    print("             / \\")
    print("    CP ──── /   \\ ──── CP")
    print("           /     \\")
    print("          / Big   \\")
    print("         /  Data   \\")
    print("        /     ▼     \\")
    print("       ◁─────────────▷")
    print("  Disponib.(A)    Tolerancia a Particiones (P)")
    print()
    print("  En Big Data, P (Tolerancia a Particiones) es INNEGOCIABLE.")
    print("  La elección real es entre C y A:")
    print("  ▸ CP (ej: HDFS, HBase) → Consistencia fuerte, puede rechazar lecturas")
    print("  ▸ AP (ej: Cassandra, DynamoDB) → Siempre disponible, consistencia eventual")


# ══════════════════════════════════════════════════════════════════════════════
# RESUMEN FINAL
# ══════════════════════════════════════════════════════════════════════════════
def resumen_mejores_practicas():
    encabezado("SÍNTESIS — MEJORES PRÁCTICAS PARA ARQUITECTURAS BIG DATA")
    mejores_practicas = [
        ("Diseñar para el crecimiento desde el primer día",
         "Escalado horizontal y desacoplamiento como principios rectores, no como parche."),
        ("Eliminar puntos únicos de falla",
         "Redundancia N+1 y monitoreo activo en cada capa crítica."),
        ("Probar el plan de recuperación, no solo documentarlo",
         "Simulacros periódicos que validen RTO y RPO reales."),
        ("Elegir la estrategia de datos según el caso de uso",
         "Balancear consistencia, disponibilidad y latencia con criterio, no por defecto."),
        ("Adoptar tendencias con criterio técnico",
         "Data Mesh, Lakehouse e IA generativa aportan valor cuando resuelven un problema real."),
    ]
    for i, (practica, desc) in enumerate(mejores_practicas, 1):
        print(f"\n  {i}. {practica}")
        print(f"     → {desc}")

    print("\n" + "═" * 65)
    print("  Maestría en Big Data y Analítica de Datos — 2026")
    print("  Docente: MSc. Ing. Marcelo Fernando Condori Mendoza")
    print("═" * 65)


# ── Punto de entrada ───────────────────────────────────────────────────────────
BLOQUES = {
    1: ("Soluciones Escalables",          demo_bloque1),
    2: ("Selección de Tecnologías",        demo_bloque2),
    3: ("Alta Disponibilidad",             demo_bloque3),
    4: ("Recuperación ante Desastres",     demo_bloque4),
    5: ("Replicación y Particionamiento",  demo_bloque5),
}


def main():
    parser = argparse.ArgumentParser(
        description="Demo integral — Arquitecturas Big Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(f"  Bloque {n}: {nombre}" for n, (nombre, _) in BLOQUES.items()),
    )
    parser.add_argument("--bloque", type=int, choices=BLOQUES.keys(),
                        help="Ejecutar solo un bloque específico (1-5)")
    parser.add_argument("--completo", action="store_true",
                        help="Habilitar logging verbose")
    args = parser.parse_args()

    if args.completo:
        logging.getLogger().setLevel(logging.INFO)

    print("\n  ╔══════════════════════════════════════════════════════════╗")
    print("  ║   DISEÑO DE ARQUITECTURAS PARA SISTEMAS DE BIG DATA     ║")
    print("  ║   Demo Aplicada — Proyecto Académico 2026               ║")
    print("  ╚══════════════════════════════════════════════════════════╝")

    if args.bloque:
        nombre, fn = BLOQUES[args.bloque]
        print(f"\n  Ejecutando: Bloque {args.bloque} — {nombre}")
        fn()
    else:
        print("\n  Ejecutando los 5 bloques del módulo...\n")
        t_total = time.perf_counter()
        for num, (nombre, fn) in BLOQUES.items():
            fn()
        elapsed = time.perf_counter() - t_total
        resumen_mejores_practicas()
        print(f"\n  ⏱️  Tiempo total de ejecución: {elapsed:.2f}s\n")


if __name__ == "__main__":
    main()
