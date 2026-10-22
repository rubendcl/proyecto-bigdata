# Bitácora de ejecución y mejoras

Documento de trabajo para preparar el informe académico en formato Word. Registrar aquí resultados observados y vincularlos con capturas reales tomadas en Visual Studio Code. Las métricas corresponden a simulaciones locales y pueden variar entre ejecuciones.

## Datos del proyecto

- Proyecto: Diseño de Arquitecturas para Sistemas de Big Data.
- Caso aplicado: monitoreo de transacciones financieras sintéticas.
- Entorno observado: Linux, Python 3.12.3, entorno virtual `.venv`.
- Fecha de las ejecuciones compartidas: 22 de octubre de 2026.
- Commit inicial: `691ea94` (`Initial commit: Big Data architecture project`).

## Ejecución base

- `python3 demo.py` completó inicialmente los bloques 1 a 4 y falló en el bloque 5 durante la comparación de replicación.
- Error observado: `ValueError: sleep length must be non-negative`.
- Causa: la latencia simulada del líder usa base de 1 ms más una variación aleatoria entre −2 y 2 ms; un valor negativo se enviaba a `time.sleep()`.
- La salida del fallo quedó en la conversación, pero no fue guardada en un archivo. No hay una captura original recuperable desde el proyecto.

## Mejora 1: limitar latencia negativa

- Archivo: `src/resiliencia/replicacion.py`.
- Cambio: limitar a cero el resultado de la latencia simulada con `max(0.0, ...)`.
- Verificación compartida: `python3 demo.py --bloque 5` terminó y comparó replicación síncrona, asíncrona y multi-líder.
- Verificación compartida: `python3 demo.py` completó los cinco bloques en 4,75 s.
- Ejecución posterior del usuario desde `.venv`: demo completa en 4,73 s.
- Capturas por incorporar al informe: fragmento del cambio y salida final de los cinco bloques.

## Ejecución del entorno y dashboard

- Se instaló el conjunto de dependencias base en `.venv`; pip informó `Successfully installed` sin errores.
- La demo completa se ejecutó desde el entorno virtual.
- El usuario informó que el dashboard Streamlit abrió y funcionó aparentemente con normalidad.
- Capturas originales del dashboard recibidas en `docs/capturas_originales/` y copiadas, sin alterar, a `docs/evidencias/`.

## Ejecución de módulos individuales

- Simulador: generó una transacción individual, lotes sintéticos y `datos/muestra_transacciones.json`.
- Circuit Breaker: mostró transición CERRADO → ABIERTO y SEMI_ABIERTO; el resultado depende del azar de la simulación.
- Replicación: las tres estrategias compararon latencias; no se reportó error tras la corrección.
- Motor batch: procesó 500 registros; en esa ejecución 497 quedaron en Silver y 3 fueron rechazados. Reportó 13.550 registros/s y KPIs Gold.
- La ejecución del motor batch emitió una advertencia por `datetime.utcnow()`. El primer cambio a `datetime.now(timezone.utc)` falló porque `timezone` no estaba importado en `transformacion_silver`; se corrigió la ubicación del import.
- Verificación posterior compartida: `python src/procesamiento/motor_batch.py` completó Bronze → Silver → Gold sin advertencias ni errores; 500 registros de entrada, 499 válidos en Silver y 1 rechazado. Throughput observado: 10.540 registros/s; las métricas dependen de cada corrida.
- La captura `docs/evidencias/26-etl-terminal-integrada-vscode.png` documenta en VS Code el primer fallo por `timezone` no importado y la ejecución repetida que completó correctamente; no muestra `DeprecationWarning` en la corrida exitosa.
- Observabilidad: registró 6 errores de 94 mensajes (6,38 %) y disparó una alerta crítica por superar el 5 %; es un resultado aleatorio de la demo.
- DRP: ejecutar como módulo desde la raíz: `python -m src.recuperacion.plan_drp`.
- Ejecutar `python src/recuperacion/plan_drp.py` dio `ModuleNotFoundError: No module named 'src'`; la invocación con `-m` resolvió el problema.
- El intento de abrir como script se debe describir como problema de invocación, no como defecto del algoritmo DRP.

## Mejora 2: distribución hash usando una clave de alta cardinalidad

- Archivo: `src/resiliencia/particionamiento.py`.
- Hallazgo base: la asignación hash tomaba primero `shard_key`, que ya contiene valores de baja cardinalidad como `shard_0` a `shard_2`, y luego los volvía a hashear.
- Resultado base compartido: de 500 registros, `shard_0=167`, `shard_1=333`, `shard_2=0`; balance 0,0 % y hotspot detectado.
- Cambio: priorizar `cliente_id` como clave para el anillo hash.
- Resultado posterior compartido: `shard_0=155`, `shard_1=169`, `shard_2=176`; balance 88,1 % y sin hotspot.
- Capturas para el informe: salida antes y después, más el fragmento de código actualizado.

## Mejora 3: replicación incremental en el simulacro DRP

- Archivo: `src/recuperacion/plan_drp.py`.
- Hallazgo base: cada ciclo copiaba solo los últimos 100 registros, incluso durante la sincronización inicial.
- Resultado base compartido con 200 registros: 100 recuperados, 100 posiblemente perdidos y una brecha de 100 registros. El RPO temporal aparecía como 0,03 min y cumplido, mostrando la diferencia entre el indicador temporal y el conteo de registros omitidos.
- Cambio: iniciar cada lote desde la cantidad ya sincronizada en standby, replicando todos los registros pendientes.
- Resultado posterior compartido: 200 registros disponibles, 0 posiblemente perdidos, RTO 0,01 min y RPO 0,03 min, ambos dentro de los objetivos declarados; cero brechas.
- El simulacro restaura intencionalmente el estado anterior al finalizar; por eso el reporte general vuelve a `region-a-activa`. El detalle del failover sí muestra `region-b-standby`.
- Capturas para el informe: salida anterior y posterior del simulacro.

## Observaciones pendientes para mejorar o explicar

- El ETL se verificó después del cambio a `datetime.now(timezone.utc)` y no volvió a mostrar `DeprecationWarning`.
- La medición hash obtenida tras la mejora fue buena en una muestra de 500, pero debe describirse como resultado de una ejecución simulada, no como garantía estadística universal.
- El RPO del DRP se calcula como tiempo desde la última sincronización. La cantidad de registros perdidos se reporta aparte; no son métricas idénticas.
- El pipeline local usa archivos JSON y la resiliencia/DRP/observabilidad son simulaciones. Evitar afirmar que hubo despliegue real multi-región, failover de DNS, clúster redundante o monitoreo productivo.
- Se copiaron 37 capturas originales a `docs/evidencias/`, superando el mínimo cuantitativo general de 15. Doce capturas muestran expresamente ejecuciones desde VS Code; hacen falta 3 capturas únicas adicionales para llegar a 15 de ese entorno.

## Evidencias visuales

El listado de archivos, descripción de cada captura y guía de inserción en el informe están en [`evidencias/INDICE_EVIDENCIAS.md`](evidencias/INDICE_EVIDENCIAS.md). Se conservaron todos los originales en `docs/capturas_originales/`.

## Estado de cambios y commits

- Commits existentes verificados con `git log`: `d72ace5` (borrador, diagramas y evidencia de regresión), `cd58e10` (verificación ETL UTC), `8f21f0d` (ignorar capturas originales), `ca23265` (bitácora y evidencias), `9a6413a` (replicación incremental DRP), `c6e0aec` (balance hash), `1d232c9` (latencia de replicación) y el inicial `691ea94`.
- El commit `d72ace5` contiene el borrador Markdown, los diagramas y las pruebas de regresión. Las revisiones de documentación realizadas después de ese commit están pendientes de guardar. No se han ejecutado comandos Git de escritura para esta revisión.
- El directorio `docs/capturas_originales/` está ignorado por Git; sus copias seleccionadas están en `docs/evidencias/`.
- No incluir `.env`, `.venv`, cachés ni JSON generados en los commits.

## Preparación del borrador del informe

- Se creó `docs/BORRADOR_INFORME.md` como documento intermedio completo para preparar el Word solicitado por el docente. Incluye portada, resumen, objetivos, alcance, método, marco teórico, descripción de la solución, implementación aplicada, comparaciones antes/después, limitaciones, conclusiones, recomendaciones, referencias y anexos.
- El contenido distingue los comportamientos ejecutados localmente de los servicios que solo están configurados o descritos, y enlaza figuras con las evidencias disponibles.
- Pendientes antes del Word: confirmar si se requieren integrantes adicionales, cohorte o ciudad en portada; revisar bibliografía y estilo requerido; confirmar que las cifras citadas corresponden a sus capturas; reunir 3 capturas auténticas adicionales de VS Code para llegar a 15; y verificar que el documento maquetado alcance 14 páginas.
- Se creó `docs/PLAN_ENTREGA.md` con una matriz de trazabilidad de la consigna, orden de cierre y criterios de aceptación para el DOCX y el ZIP. La portada consigna la Universidad Amazónica de Pando, el Módulo 10 y el autor Ruben Cabrera. La imagen del módulo también muestra cohorte/código y Sucre; confirmar si deben añadirse esos datos.

## Pruebas de regresión para mejoras de resiliencia

- Se extrajo `PlanDRP._replicar_pendientes()` para aislar la sincronización incremental del ciclo periódico y poder comprobarla sin iniciar el hilo ni esperar la latencia simulada.
- Se creó `tests/test_mejoras_aplicadas.py` con tres casos: latencia simulada nunca negativa, distribución de 500 clientes entre los tres shards y replicación DRP completa/incremental sin duplicados.
- Se actualizó el comando de prueba específica en `README.md` a `pytest tests/test_mejoras_aplicadas.py -v`. El usuario compartió una corrida exitosa descrita a continuación.
- Ejecución compartida por el usuario desde el entorno `.venv`: `python -m pytest tests/test_mejoras_aplicadas.py -v` recogió 3 pruebas y las 3 pasaron (`3 passed in 0.16s`, Python 3.12.3, pytest 9.1.1).
- El usuario indicó que la captura está en `docs/capturas_originales/141 Terminal_VSCode.png`. Se verificó el origen (58 KB), se copió sin modificar a `docs/evidencias/27-pruebas-regresion-vscode.png` y se inspeccionó visualmente: muestra `3 passed in 0.16s` y los tres nombres de pruebas.

## Demo integral y módulos repetidos desde VS Code

- El usuario compartió la salida de `python demo.py`; terminó en 4,69 s. En la corrida, el bloque ETL de 300 entradas presentó 294 registros Gold y monto total USD 613.725,64. El Circuit Breaker registró 12 llamadas: 2 éxitos, 4 fallos y 6 rechazos. El simulacro DRP recuperó 150 registros y no reportó pérdidas. El sharding hash distribuyó 600 registros como 183/211/206 (balance 86,7 %, sin hotspot).
- En ejecuciones individuales compartidas: el particionador procesó 500 registros y distribuyó 150/189/161 (79,4 %, sin hotspot); la ejecución DRP recuperó 200, perdió 0 (RTO 0,01 min, RPO 0,03 min); ETL procesó 500, guardó 494 en Silver y rechazó 6 (12.043 registros/s); observabilidad informó 98 mensajes, 2 errores (2,04 %), 8 alertas de fraude y 0 alertas activas.
- Se copiaron sin modificar las capturas originales 145–154 a `docs/evidencias/28-demo-vscode-bloque1-2.png` hasta `37-observabilidad-vscode.png`. Las imágenes se inspeccionaron visualmente y muestran los seis segmentos consecutivos de la demo y las cuatro ejecuciones individuales.
- Se actualizaron el índice, el borrador, el plan de entrega y la salida HTML. Estado vigente: 37 capturas organizadas; 12 son evidencia explícita de VS Code y quedan 3 por recopilar para alcanzar 15.

## Diagramas del informe

- Se prepararon cinco diagramas vectoriales SVG: arquitectura (Figura 1), flujo ETL (Figura 27), secuencia DRP (Figura 28), ciclo de Circuit Breaker (Figura 29) y comparación de sharding antes/después (Figura 30).
- La arquitectura distingue ejecución local observada de servicios configurados que no se levantaron. Los otros diagramas reflejan comportamientos simulados y resultados de corridas sintéticas; sus pies y notas mantienen esos límites explícitos.
- Se reemplazó el bloque Mermaid del flujo lógico por la Figura 1 en SVG y se enlazaron las cuatro figuras adicionales desde el borrador. Se añadieron notas de uso en `docs/figuras/README.md`; falta revisar la presentación al maquetar el DOCX y, si la versión de Word no admite SVG, exportar PNG sin modificar el contenido.

## Ajuste de referencias bibliográficas

- A solicitud del usuario, se redujo la bibliografía del borrador de diez a seis fuentes principales: Lambda/Kappa, capas medallion, CAP, Circuit Breaker, recuperación ante desastres (NIST) y fechas UTC en Python.
- Se renumeraron las citas en el cuerpo y se simplificaron los pasajes de Kafka, Delta Lake y observabilidad que dependían de fuentes retiradas, para evitar citas sin referencia. También se quitó la referencia de Airflow que no tenía cita en el texto.
- Se reemplazaron los espacios finales de las líneas de portada por saltos de línea explícitos para que `git diff --check` no marque esos renglones.
- Se debe regenerar la vista HTML cada vez que se cierre una actualización del Markdown; ambas versiones se actualizaron con esta reducción.
