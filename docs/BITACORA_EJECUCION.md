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
- Se copiaron 26 capturas originales a `docs/evidencias/`, superando el mínimo cuantitativo de 15. Una captura muestra expresamente VS Code y la terminal integrada; revisar la nota del índice para asegurar suficientes figuras en ese formato antes de cerrar el informe.

## Evidencias visuales

El listado de archivos, descripción de cada captura y guía de inserción en el informe están en [`evidencias/INDICE_EVIDENCIAS.md`](evidencias/INDICE_EVIDENCIAS.md). Se conservaron todos los originales en `docs/capturas_originales/`.

## Estado de cambios y commits

- Commits existentes verificados con `git log`: `1d232c9` (latencia de replicación), `c6e0aec` (balance del sharding hash), `9a6413a` (replicación incremental DRP), `ca23265` (bitácora inicial, evidencias e ignorados locales) y `8f21f0d` (ignorar carpeta de capturas originales), además del commit inicial `691ea94`.
- Cambios pendientes de commit al 22 de octubre de 2026: `src/procesamiento/motor_batch.py`, `README.md` y esta bitácora. La nueva evidencia 26 también se debe guardar en un commit junto con el índice actualizado.
- Se ejecutó `git status` y se confirmó `master`; el árbol contenía únicamente esos tres archivos modificados y el nuevo PNG antes de incorporarlo a seguimiento. No se ejecutaron `git add` ni `git commit` durante esta revisión.
- El directorio `docs/capturas_originales/` está ignorado por Git; sus copias seleccionadas están en `docs/evidencias/`.
- No incluir `.env`, `.venv`, cachés ni JSON generados en los commits.
