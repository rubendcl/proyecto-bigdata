# Índice de evidencias

Se copiaron aquí 25 capturas desde `docs/capturas_originales/`. Los archivos originales permanecen sin cambios. Las capturas son evidencia de las ejecuciones e interfaces mostradas; los valores aleatorios y las métricas de simulación pueden variar entre corridas.

| N.º | Archivo | Qué documenta |
|---:|---|---|
| 1 | `01-inventario-docker.png` | Inventario inicial de Docker sin imágenes ni contenedores del proyecto. |
| 2 | `02-inicializacion-git.png` | Inicialización local de Git y archivos por agregar. |
| 3 | `03-instalacion-dependencias.png` | Instalación de dependencias del entorno virtual. |
| 4 | `04-demo-bloque1-inicio.png` | Inicio de demo completa; generación de transacciones y ETL. |
| 5 | `05-demo-bloques2-3.png` | Selección de tecnologías y comienzo del Circuit Breaker. |
| 6 | `06-demo-bloque3-circuit-breaker.png` | Transiciones del Circuit Breaker y métricas de alta disponibilidad. |
| 7 | `07-demo-bloque4-inicio.png` | BIA y sitios hot/warm/cold del DRP. |
| 8 | `08-demo-linea-base-drp-y-sharding.png` | Ejecución de línea base: pérdidas DRP y shard hash vacío. |
| 9 | `09-demo-replicacion-y-cap.png` | Comparación de replicación y explicación CAP. |
| 10 | `10-demo-finalizacion.png` | Síntesis y finalización de demo completa en 4,73 s. |
| 11 | `11-dashboard-servidor-streamlit.png` | Arranque del servidor Streamlit y dirección local. |
| 12 | `12-dashboard-etl.png` | Vista inicial del dashboard y resultados ETL. |
| 13 | `13-dashboard-graficos.png` | Gráficos de tipos de transacción y regiones. |
| 14 | `14-dashboard-circuit-breaker.png` | Dashboard interactivo de Circuit Breaker. |
| 15 | `15-dashboard-sharding-linea-base.png` | Vista de línea base con shard vacío y hotspot. |
| 16 | `16-dashboard-drp-linea-base.png` | Vista de línea base del DRP, con pérdida reportada. |
| 17 | `17-dashboard-observabilidad.png` | Métricas, errores, alertas y latencias del dashboard. |
| 18 | `18-modulo-simulador.png` | Ejecución individual del simulador de transacciones. |
| 19 | `19-modulo-circuit-breaker.png` | Ejecución individual y métricas del Circuit Breaker. |
| 20 | `20-modulo-sharding-linea-base-y-replicacion.png` | Sharding hash de línea base y comienzo de la comparación de replicación. |
| 21 | `21-modulo-etl-y-advertencia-utc.png` | Pipeline ETL, registros rechazados, KPIs y advertencia `utcnow()`. |
| 22 | `22-modulo-observabilidad-e-invocacion-drp.png` | Observabilidad y error de invocación directa del módulo DRP. |
| 23 | `23-modulo-drp-linea-base.png` | Simulacro DRP de línea base: 100 registros recuperados y 100 perdidos. |
| 24 | `24-modulo-sharding-mejorado.png` | Resultado posterior: tres shards poblados, balance 88,1 %, sin hotspot. |
| 25 | `25-modulo-drp-mejorado.png` | Resultado posterior: 200 registros recuperados, cero perdidos y cero brechas. |

## Cómo usarlas en el informe

- Las capturas 8, 15, 16, 20 y 23 son resultados de línea base, anteriores a las mejoras de sharding y DRP.
- Las capturas 24 y 25 muestran las ejecuciones posteriores a las mejoras correspondientes.
- Las capturas 4 a 10 son fragmentos consecutivos de una misma ejecución de demo, no siete ejecuciones distintas.
- Numerar las figuras del informe según el orden en que se citen; no es necesario insertar las 25 si algunas resultan repetitivas, pero conservar al menos 15 capturas pertinentes.
- Añadir pies de figura que expliquen qué comando, sección o resultado se observa. No recortar ni retocar las capturas para alterar su contenido.
- Para afirmar cumplimiento de la consigna docente sobre Visual Studio Code, comprobar que las capturas incluidas efectivamente muestran VS Code o su terminal integrada; varias imágenes son de terminal Linux y las del dashboard son del navegador.
