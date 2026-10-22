"""
app_streamlit.py
================
Dashboard de monitoreo en tiempo real para el sistema Big Data.
Visualiza métricas de observabilidad, estado de la arquitectura,
resultados del pipeline ETL y el estado del Circuit Breaker.

Ejecutar con:
    streamlit run dashboard/app_streamlit.py
"""

import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Añadir raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingesta.simulador_transacciones import generar_lote
from src.procesamiento.motor_batch import MotorBatch
from src.resiliencia.circuit_breaker import CircuitBreaker, CircuitoBiertoError
from src.resiliencia.particionamiento import GestorParticionamiento
from src.recuperacion.plan_drp import PlanDRP
from src.monitoreo.observabilidad import SistemaObservabilidad

# ── Configuración de la página ─────────────────────────────────────────────────
st.set_page_config(
    page_title="BigData — Arquitecturas Robustas",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paleta de colores (inspirada en la presentación) ──────────────────────────
VERDE_PRIMARIO = "#1A6B42"
VERDE_CLARO = "#4CAF82"
FONDO_CARD = "#F0F7F4"

st.markdown(
    f"""
    <style>
        .stMetric {{ background-color: {FONDO_CARD}; border-radius: 8px; padding: 8px; }}
        .main-title {{ color: {VERDE_PRIMARIO}; font-size: 2rem; font-weight: 700; }}
        .section-header {{ color: {VERDE_PRIMARIO}; border-bottom: 2px solid {VERDE_CLARO}; padding-bottom: 4px; }}
        .alerta-card {{ background: #FFF3CD; border-left: 4px solid #FFC107; padding: 8px; border-radius: 4px; margin: 4px 0; }}
        .ok-card {{ background: #D4EDDA; border-left: 4px solid #28A745; padding: 8px; border-radius: 4px; }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Inicialización de estado (caché de sesión) ─────────────────────────────────
@st.cache_resource
def obtener_observabilidad():
    return SistemaObservabilidad("bigdata-dashboard")


@st.cache_resource
def obtener_circuit_breaker():
    return CircuitBreaker("kafka-demo", umbral_fallos=4, timeout_s=10)


@st.cache_resource
def obtener_gestor_shard():
    return GestorParticionamiento(num_shards=3, estrategia="hash")


@st.cache_resource
def obtener_drp():
    return PlanDRP()


obs = obtener_observabilidad()
cb = obtener_circuit_breaker()
gestor = obtener_gestor_shard()
drp = obtener_drp()


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"<h2 style='color:{VERDE_PRIMARIO}'>🏗️ Control Panel</h2>", unsafe_allow_html=True)
    st.markdown("**Maestría en Big Data y Analítica de Datos**")
    st.divider()

    n_transacciones = st.slider("Transacciones a generar", 50, 2000, 300, 50)
    prob_fraude = st.slider("Probabilidad de fraude (%)", 1, 20, 3) / 100
    prob_fallo = st.slider("Probabilidad de fallo servicio (%)", 0, 80, 30) / 100
    estrategia_shard = st.selectbox("Estrategia de Sharding", ["hash", "geografica"])

    st.divider()
    auto_refresh = st.checkbox("Auto-refresh (5s)", value=False)
    if auto_refresh:
        time.sleep(5)
        st.rerun()


# ── Encabezado ─────────────────────────────────────────────────────────────────
st.markdown(
    "<h1 class='main-title'>🏗️ Arquitecturas Big Data — Dashboard de Monitoreo</h1>",
    unsafe_allow_html=True,
)
st.caption(
    f"Docente: MSc. Ing. Marcelo Fernando Condori Mendoza  |  "
    f"Actualizado: {datetime.now().strftime('%H:%M:%S')}"
)
st.divider()


# ════════════════════════════════════════════════════════════════════
# BLOQUE 1 — PIPELINE ETL EN TIEMPO REAL
# ════════════════════════════════════════════════════════════════════
st.markdown("<h2 class='section-header'>📦 Bloque 1 · Pipeline ETL (Arquitectura Lambda)</h2>", unsafe_allow_html=True)

col_btn1, col_btn2, col_btn3 = st.columns(3)
ejecutar_etl = col_btn1.button("▶ Ejecutar Pipeline ETL", type="primary", use_container_width=True)
simular_streaming = col_btn2.button("🔄 Simular Streaming", use_container_width=True)
limpiar = col_btn3.button("🗑️ Limpiar resultados", use_container_width=True)

if ejecutar_etl:
    with st.spinner(f"Ejecutando pipeline ETL con {n_transacciones} transacciones..."):
        motor = MotorBatch(usar_spark=False)
        transacciones = generar_lote(n_transacciones, prob_fraude)
        resultado = motor.ejecutar_pipeline_etl(transacciones)
        st.session_state["resultado_etl"] = resultado
        st.session_state["transacciones"] = transacciones
        obs.registros_batch.incrementar(n_transacciones)

if "resultado_etl" in st.session_state:
    r = st.session_state["resultado_etl"]
    kpis = r.get("kpis", {})

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("📊 Registros", f"{r['registros_entrada']:,}")
    col2.metric("⏱️ Duración", f"{r['duracion_s']:.2f}s")
    col3.metric("🚀 Throughput", f"{r['throughput_registros_s']:,.0f} rec/s")
    col4.metric("💰 Monto Total", f"${kpis.get('monto_total_usd', 0):,.0f} USD")
    col5.metric(
        "🚨 Tasa Fraude",
        f"{kpis.get('tasa_fraude_pct', 0):.2f}%",
        delta=f"{'⬆️ ALERTA' if kpis.get('tasa_fraude_pct', 0) > 5 else '✅ Normal'}",
        delta_color="inverse",
    )

    # Capas del Lakehouse
    st.markdown("##### Capas del Lakehouse (Bronze → Silver → Gold)")
    capas = st.columns(3)
    with capas[0]:
        st.info(f"🥉 **Bronze (Raw)**\n\n{r['registros_entrada']:,} registros\nDatos crudos sin transformar")
    with capas[1]:
        registros_silver = int(r['registros_entrada'] * 0.99)
        st.warning(f"🥈 **Silver (Limpio)**\n\n{registros_silver:,} registros\nValidados y enriquecidos")
    with capas[2]:
        st.success(f"🥇 **Gold (Agregado)**\n\nKPIs calculados\nListos para analítica")

    # Distribución de transacciones
    if "transacciones" in st.session_state:
        txs = st.session_state["transacciones"]
        df = pd.DataFrame(txs)

        col_g1, col_g2 = st.columns(2)
        with col_g1:
            fig_tipo = px.bar(
                df.groupby("tipo").size().reset_index(name="count"),
                x="tipo", y="count",
                title="Transacciones por Tipo",
                color_discrete_sequence=[VERDE_PRIMARIO],
            )
            st.plotly_chart(fig_tipo, use_container_width=True)

        with col_g2:
            df_region = df.groupby("region").agg(
                total=("monto", "count"),
                monto_total=("monto", "sum"),
                fraudes=("es_fraude_potencial", "sum"),
            ).reset_index()
            fig_region = px.bar(
                df_region, x="region", y="total",
                title="Transacciones por Región (Particionamiento Geográfico)",
                color="fraudes",
                color_continuous_scale="RdYlGn_r",
            )
            st.plotly_chart(fig_region, use_container_width=True)

st.divider()


# ════════════════════════════════════════════════════════════════════
# BLOQUE 2 — CIRCUIT BREAKER (Alta Disponibilidad)
# ════════════════════════════════════════════════════════════════════
st.markdown("<h2 class='section-header'>⚡ Bloque 2 · Circuit Breaker (Alta Disponibilidad)</h2>", unsafe_allow_html=True)

col_cb1, col_cb2 = st.columns([1, 2])

with col_cb1:
    estado_color = {
        "CERRADO": "🟢",
        "ABIERTO": "🔴",
        "SEMI_ABIERTO": "🟡",
    }
    estado_cb = cb.estado.value
    st.markdown(f"### Estado: {estado_color.get(estado_cb, '⚪')} {estado_cb}")

    if st.button("📡 Simular llamadas al servicio", use_container_width=True):
        resultados_cb = []
        for _ in range(10):
            try:
                def servicio_inestable():
                    if random.random() < prob_fallo:
                        raise ConnectionError("Timeout del servicio")
                    return {"latencia_ms": random.randint(20, 100)}

                res = cb.ejecutar(servicio_inestable)
                resultados_cb.append(("✅ OK", res["latencia_ms"]))
                obs.mensajes_kafka_procesados.incrementar()
                obs.latencia_ms.observar(res["latencia_ms"])
            except CircuitoBiertoError:
                resultados_cb.append(("🔴 ABIERTO", 0))
                obs.fallos_circuit_breaker.incrementar()
            except Exception as e:
                resultados_cb.append((f"❌ {type(e).__name__}", 0))
                obs.errores_procesamiento.incrementar()

        st.session_state["resultados_cb"] = resultados_cb

    if st.button("🔄 Resetear circuito", use_container_width=True):
        cb.resetear()
        st.success("Circuito reseteado manualmente.")

    metricas_cb = cb.obtener_metricas()
    st.metric("Llamadas totales", metricas_cb["total_llamadas"])
    st.metric("Tasa de éxito", f"{metricas_cb['tasa_exito_pct']:.1f}%")
    st.metric("Fallos en ventana", metricas_cb["fallos_en_ventana"])
    st.metric("Rechazos (circuito abierto)", metricas_cb["total_rechazos"])

with col_cb2:
    if "resultados_cb" in st.session_state:
        st.markdown("##### Últimas 10 llamadas al servicio")
        for i, (estado, lat) in enumerate(st.session_state["resultados_cb"], 1):
            if "OK" in estado:
                st.markdown(f'<div class="ok-card">[{i:02d}] {estado} — {lat}ms</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="alerta-card">[{i:02d}] {estado}</div>', unsafe_allow_html=True)

    # Historial de transiciones del circuito
    historial = metricas_cb.get("historial_transiciones", [])
    if historial:
        st.markdown("##### Historial de transiciones")
        df_hist = pd.DataFrame(historial)
        df_hist["timestamp"] = pd.to_datetime(df_hist["timestamp"], unit="s")
        st.dataframe(df_hist[["de", "a", "timestamp"]], use_container_width=True, hide_index=True)

st.divider()


# ════════════════════════════════════════════════════════════════════
# BLOQUE 3 — PARTICIONAMIENTO (Sharding)
# ════════════════════════════════════════════════════════════════════
st.markdown("<h2 class='section-header'>🗂️ Bloque 3 · Particionamiento de Datos (Sharding)</h2>", unsafe_allow_html=True)

if st.button("🔀 Distribuir transacciones entre shards", use_container_width=False):
    nuevo_gestor = GestorParticionamiento(num_shards=3, estrategia=estrategia_shard)
    transacciones_shard = generar_lote(500, prob_fraude)
    nuevo_gestor.insertar_lote(transacciones_shard)
    st.session_state["stats_shard"] = nuevo_gestor.estadisticas()

if "stats_shard" in st.session_state:
    stats = st.session_state["stats_shard"]
    col_s1, col_s2 = st.columns([1, 2])

    with col_s1:
        st.metric("Total registros", f"{stats['total_registros']:,}")
        st.metric("Balance de distribución", f"{stats['balance_pct']}%")
        if stats["hotspot_detectado"]:
            st.error("⚠️ Hotspot detectado. Revisar estrategia de sharding.")
        else:
            st.success("✅ Distribución balanceada. Sin hotspots.")

    with col_s2:
        df_shards = pd.DataFrame([
            {"Shard": k, "Registros": v, "Porcentaje": round(v / stats['total_registros'] * 100, 1)}
            for k, v in stats["distribucion"].items()
        ])
        fig_shard = px.bar(
            df_shards, x="Shard", y="Registros",
            title=f"Distribución de datos ({estrategia_shard})",
            text="Porcentaje",
            color="Registros",
            color_continuous_scale=[[0, "#C8E6C9"], [1, VERDE_PRIMARIO]],
        )
        fig_shard.update_traces(texttemplate="%{text}%", textposition="outside")
        st.plotly_chart(fig_shard, use_container_width=True)

st.divider()


# ════════════════════════════════════════════════════════════════════
# BLOQUE 4 — DRP / RTO / RPO
# ════════════════════════════════════════════════════════════════════
st.markdown("<h2 class='section-header'>🛡️ Bloque 4 · Recuperación ante Desastres (DRP)</h2>", unsafe_allow_html=True)

col_drp1, col_drp2 = st.columns(2)

with col_drp1:
    st.markdown("##### Sitios de respaldo (Multi-región)")
    sitios = [
        {"Región": "Región A", "Tipo": "HOT ✅", "Estado": "ACTIVO 🟢", "Latencia": "< 1 min"},
        {"Región": "Región B", "Tipo": "WARM 🟡", "Estado": "STANDBY 🟡", "Latencia": "5-15 min"},
        {"Región": "Región C", "Tipo": "COLD ❄️", "Estado": "BACKUP 🔵", "Latencia": "> 1 hora"},
    ]
    st.dataframe(pd.DataFrame(sitios), use_container_width=True, hide_index=True)

    st.markdown("##### Procesos críticos (Análisis de Impacto - BIA)")
    procesos = [
        {"Proceso": "Procesamiento pagos", "RTO": "5 min", "RPO": "1 min", "Prioridad": "🔴 CRÍTICO"},
        {"Proceso": "Detección fraude", "RTO": "10 min", "RPO": "5 min", "Prioridad": "🔴 CRÍTICO"},
        {"Proceso": "Pipeline Kafka", "RTO": "15 min", "RPO": "5 min", "Prioridad": "🟠 ALTO"},
        {"Proceso": "Data Warehouse", "RTO": "30 min", "RPO": "15 min", "Prioridad": "🟠 ALTO"},
    ]
    st.dataframe(pd.DataFrame(procesos), use_container_width=True, hide_index=True)

with col_drp2:
    if st.button("🧪 Ejecutar Simulacro DRP", type="primary", use_container_width=True):
        with st.spinner("Ejecutando simulacro de recuperación ante desastres..."):
            # Cargar datos al DRP
            txs_drp = generar_lote(200)
            for tx in txs_drp:
                drp.registrar_dato(tx)
            drp.iniciar_replicacion()
            time.sleep(1)
            resultado_sim = drp.ejecutar_simulacro("fallo_datacenter")
            drp.detener_replicacion()
            st.session_state["resultado_drp"] = resultado_sim

    if "resultado_drp" in st.session_state:
        r = st.session_state["resultado_drp"]
        fo = r.get("resultado_failover", {})

        col_r1, col_r2 = st.columns(2)
        rto_ok = fo.get("rto_cumplido", False)
        rpo_ok = fo.get("rpo_cumplido", False)

        col_r1.metric(
            "RTO Real",
            f"{fo.get('rto_real_minutos', 0):.2f} min",
            delta=f"Objetivo: {fo.get('rto_objetivo_minutos', 15)} min",
            delta_color="normal" if rto_ok else "inverse",
        )
        col_r2.metric(
            "RPO Real",
            f"{fo.get('rpo_real_minutos', 0):.2f} min",
            delta=f"Objetivo: {fo.get('rpo_objetivo_minutos', 5)} min",
            delta_color="normal" if rpo_ok else "inverse",
        )

        # Semáforo de cumplimiento
        for label, cumplido in [("RTO", rto_ok), ("RPO", rpo_ok)]:
            if cumplido:
                st.markdown(f'<div class="ok-card">✅ {label} cumplido</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="alerta-card">❌ {label} NO cumplido</div>', unsafe_allow_html=True)

        brechas = r.get("brechas_detectadas", [])
        recomendaciones = r.get("recomendaciones", [])
        if brechas:
            st.error("**Brechas detectadas:**\n" + "\n".join(f"• {b}" for b in brechas))
        if recomendaciones:
            st.info("**Recomendaciones:**\n" + "\n".join(f"• {rec}" for rec in recomendaciones))

st.divider()


# ════════════════════════════════════════════════════════════════════
# BLOQUE 5 — OBSERVABILIDAD
# ════════════════════════════════════════════════════════════════════
st.markdown("<h2 class='section-header'>🔍 Bloque 5 · Observabilidad y Monitoreo Proactivo</h2>", unsafe_allow_html=True)

snap = obs.snapshot()
col_o1, col_o2, col_o3, col_o4 = st.columns(4)
col_o1.metric("📨 Mensajes Kafka", f"{snap['contadores']['mensajes_kafka']:,}")
col_o2.metric("❌ Errores", f"{snap['contadores']['errores']:,}")
col_o3.metric("🚨 Alertas Fraude", f"{snap['contadores']['alertas_fraude']:,}")
col_o4.metric("📈 Tasa Error", f"{snap['tasa_error_pct']:.2f}%")

if snap["latencia"]:
    lat = snap["latencia"]
    st.markdown("##### Distribución de Latencias")
    fig_lat = go.Figure()
    percentiles = {"P50": lat.get("p50", 0), "P90": lat.get("p90", 0), "P95": lat.get("p95", 0), "P99": lat.get("p99", 0)}
    fig_lat.add_trace(go.Bar(
        x=list(percentiles.keys()),
        y=list(percentiles.values()),
        marker_color=[VERDE_PRIMARIO, VERDE_CLARO, "#FFA726", "#EF5350"],
        text=[f"{v:.0f}ms" for v in percentiles.values()],
        textposition="outside",
    ))
    fig_lat.add_hline(y=500, line_dash="dash", line_color="red", annotation_text="Umbral 500ms")
    fig_lat.update_layout(title="Latencia de procesamiento (percentiles)", yaxis_title="ms", height=300)
    st.plotly_chart(fig_lat, use_container_width=True)

st.markdown("---")
st.caption("🏗️ Proyecto académico — Maestría en Big Data y Analítica de Datos 2026 · MSc. Ing. Marcelo Fernando Condori Mendoza")
