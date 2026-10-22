# Índice de evidencias

Se copiaron aquí 37 capturas desde `docs/capturas_originales/`. Los archivos originales permanecen sin cambios. Las capturas son evidencia de las ejecuciones e interfaces mostradas; los valores aleatorios y las métricas de simulación pueden variar entre corridas.

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
| 26 | `26-etl-terminal-integrada-vscode.png` | Terminal integrada de VS Code: primer intento con `NameError` por el import de `timezone` y ejecución posterior exitosa del ETL con 500 entradas, 499 válidas y 1 rechazada. |
| 27 | `27-pruebas-regresion-vscode.png` | Terminal integrada de VS Code: ejecución de `python -m pytest tests/test_mejoras_aplicadas.py -v`; 3 pruebas pasaron en 0,16 s. |
| 28 | `28-demo-vscode-bloque1-2.png` | Demo completa en VS Code: transacción sintética, escalado por lotes, ETL Bronze/Silver/Gold y comienzo de selección de tecnologías. |
| 29 | `29-demo-vscode-bloque2-3.png` | Demo completa en VS Code: criterios TCO y ejecución de llamadas del Circuit Breaker. |
| 30 | `30-demo-vscode-bloque3-4.png` | Demo completa en VS Code: métricas de Circuit Breaker, observabilidad y comienzo del bloque DRP. |
| 31 | `31-demo-vscode-bloque4-5.png` | Demo completa en VS Code: simulacro DRP con 150 registros recuperados, ninguno perdido, y comienzo de sharding. |
| 32 | `32-demo-vscode-bloque5-sintesis.png` | Demo completa en VS Code: particionamiento, estrategias de replicación, CAP y comienzo de síntesis. |
| 33 | `33-demo-vscode-sintesis-final.png` | Demo completa en VS Code: recomendaciones de arquitectura y tiempo total de 4,69 s. |
| 34 | `34-sharding-vscode.png` | Ejecución individual en VS Code: 500 registros distribuidos en 150/189/161; balance hash de 79,4 % y sin hotspot. |
| 35 | `35-drp-vscode.png` | Ejecución individual del DRP en VS Code: 200 registros recuperados, cero perdidos, RTO 0,01 min y RPO 0,03 min. |
| 36 | `36-etl-vscode.png` | Ejecución individual del ETL en VS Code: 500 entradas, 494 válidas, 6 rechazadas, throughput de 12.043 registros/s y KPIs Gold. |
| 37 | `37-observabilidad-vscode.png` | Ejecución individual de observabilidad en VS Code: 98 mensajes, 2 errores (2,04 %), 8 alertas de fraude y cero alertas activas. |

## Cómo usarlas en el informe

- Las capturas 8, 15, 16, 20 y 23 son resultados de línea base, anteriores a las mejoras de sharding y DRP.
- Las capturas 24 y 25 muestran las ejecuciones posteriores a las mejoras correspondientes.
- La captura 26 muestra Visual Studio Code y su terminal integrada, además del intento fallido durante la corrección y la ejecución exitosa posterior.
- La captura 27 muestra la suite de regresión ejecutada desde VS Code; los tres casos pasaron en 0,16 s.
- Las capturas 28–33 son fragmentos consecutivos de una misma ejecución de `python demo.py`, no seis ejecuciones diferentes.
- Las capturas 34–37 muestran ejecuciones individuales de particionamiento, DRP, ETL y observabilidad. Las métricas dependen de datos sintéticos y de cada corrida.
- Las capturas 4 a 10 son fragmentos consecutivos de una misma ejecución de demo, no siete ejecuciones distintas.
- Numerar las figuras del informe según el orden en que se citen; no es necesario insertar las 25 si algunas resultan repetitivas, pero conservar al menos 15 capturas pertinentes.
- Añadir pies de figura que expliquen qué comando, sección o resultado se observa. No recortar ni retocar las capturas para alterar su contenido.
- Doce capturas explícitas corresponden a ejecución en VS Code (26–37). Confirmar que al menos 15 figuras del informe sean capturas únicas de ejecución en VS Code, según la interpretación literal de la consigna; por ahora faltan 3 capturas adicionales para llegar a esa meta.
