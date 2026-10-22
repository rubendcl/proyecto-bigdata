# Plan de cierre y verificación de la entrega

Este plan traduce la consigna del docente a entregables verificables. Sirve como lista de control para el grupo; no sustituye la revisión final del docente ni implica que los elementos pendientes ya estén completados.

## Requisitos y estado actual

| Requisito docente | Evidencia o estado actual | Acción de cierre |
|---|---|---|
| Informe Word `.docx` completo | Existe el borrador integral `BORRADOR_INFORME.md` y su vista HTML. No existe aún el Word final. | Completar portada, revisar texto y fuentes, exportar a DOCX y verificar el archivo página por página. |
| Mínimo de 14 páginas, incluyendo imágenes/figuras/diagramas | El borrador incluye 37 capturas catalogadas y cinco diagramas SVG; la paginación aún no se midió en DOCX. | Aplicar estilos, márgenes, tamaño de letra e interlineado institucionales; comprobar que Word muestra al menos 14 páginas sin inflar artificialmente el texto. |
| Introducción, objetivos, marco teórico, implementación aplicada, conclusiones y recomendaciones | Las seis secciones están en el borrador. | Revisión de redacción y coherencia final; añadir el formato bibliográfico solicitado si el docente lo especifica. |
| Implementación aplicada exhaustiva y detallada | El borrador describe entorno, módulos, ejecuciones, hallazgos, cambios y resultados. | Completar capturas faltantes y revisar que cada cifra citada corresponda a una ejecución identificable. |
| Al menos 15 capturas de ejecución del proyecto en VS Code, con resultados intermedios y finales | Hay 37 capturas catalogadas; las capturas 26–37 muestran explícitamente ejecuciones desde VS Code. | Capturar al menos 3 pantallas adicionales en VS Code y seleccionar al menos 15 pertinentes para el DOCX. Mantener salida legible, comando visible y relación con el texto. |
| Proyecto completo modificado/actualizado, con todos los recursos | Código, configuración, README, fuentes y evidencias están en el repositorio. Servicios Docker/Airflow no se ejecutaron; el informe ya declara ese alcance. | Revisar que no se incluyan `.env`, `.venv`, cachés ni datos de ejecución innecesarios; comprobar README y estructura del ZIP. |
| Archivo MD con instrucciones de ejecución | Existe `README.md`. La demo local y los comandos de módulos están documentados. | Revisar los comandos desde una copia limpia del ZIP y separar con claridad ejecución local y stack Docker opcional. |
| Proyecto comprimido ZIP o RAR | Pendiente. | Crear ZIP desde la raíz, sin `.git`, `.venv`, secretos ni cachés; listar su contenido y probar extracción en una carpeta temporal. |
| Originalidad y mejoras documentadas | Cuatro cambios aplicados y comparados antes/después en el borrador y la bitácora. | Conservar explicaciones y evidencia auténtica; no presentar resultados simulados como mediciones de infraestructura real. |

## Orden recomendado de trabajo

1. **Cerrar las capturas en VS Code.** Usar el terminal integrado abierto en la raíz del proyecto. Capturar ejecuciones completas y legibles de la demo, ETL, sharding antes/después, DRP antes/después, Circuit Breaker, replicación, observabilidad y dashboard. No fabricar ni alterar salidas. Guardar capturas nuevas en `docs/evidencias/` y registrar su descripción en `docs/evidencias/INDICE_EVIDENCIAS.md`.
2. **Completar datos de portada.** Institución, autoría/grupo, ciudad y fecha efectiva de presentación.
3. **Revisar evidencia contra resultados.** Usar `docs/BITACORA_EJECUCION.md` como fuente de trazabilidad. Para cada tabla o cifra, identificar comando, corrida y captura; señalar que los datos sintéticos pueden variar.
4. **Congelar el contenido del informe.** Revisar consistencia, ortografía, referencias y pies de figura. Mantener las limitaciones explícitas: los resultados probados son locales/simulados; Docker, Kafka, Airflow, Spark y Delta no fueron verificados en esta fase.
5. **Generar DOCX.** Aplicar estilos de título de Word, tabla de contenido automática, numeración de páginas y pies de figura. Colocar al menos 15 capturas pertinentes de VS Code y confirmar un mínimo de 14 páginas.
6. **Preparar paquete del proyecto.** Revisar README; listar el contenido del ZIP; excluir secretos, entorno virtual, `.git` y archivos temporales; probar extracción y seguir las instrucciones en un entorno limpio.
7. **Revisión final de ambos entregables.** Abrir el DOCX en Word o LibreOffice y revisar saltos, imágenes, tablas, encabezados, fuentes y paginación. Abrir el ZIP extraído y confirmar código, recursos, instrucciones y capturas.

## Guion de captura para la evidencia que falta

Desde VS Code, abrir una terminal integrada en la raíz del proyecto y activar `.venv`. Tomar capturas individuales o grupos de pantallas que demuestren:

- Entorno y estructura del proyecto, con `python --version` y `git status`.
- Demo completa y resultados intermedios por bloques.
- Pipeline ETL Bronze → Silver → Gold y KPIs.
- Distribución hash línea base y distribución mejorada, vinculadas a sus cambios.
- Simulacro DRP línea base y mejorado, mostrando recuperados/perdidos y RTO/RPO.
- Circuit Breaker, replicación y observabilidad.
- Dashboard abierto y sus vistas principales.
- Fragmentos de código modificados junto a la ejecución correspondiente, cuando ayuden a explicar la mejora.

Las capturas `26-etl-terminal-integrada-vscode.png` a `37-observabilidad-vscode.png` cuentan como 12 evidencias explícitas de VS Code. La meta operativa es incorporar al menos 3 capturas únicas adicionales que también muestren VS Code. Reutilizar una captura para varias figuras no aumenta el número de capturas únicas.

## Criterios de aceptación antes de entregar

- [ ] El DOCX abre sin reparación y tiene 14 páginas o más.
- [ ] Incluye las seis secciones mínimas del docente y referencias coherentes con las citas.
- [ ] Contiene por lo menos 15 capturas únicas y auténticas tomadas en VS Code, con pies legibles y resultados intermedios/finales.
- [ ] Los cambios del proyecto se explican con hallazgo, solución y resultado observado.
- [ ] Se diferencia claramente la simulación del despliegue real.
- [ ] El ZIP se extrae correctamente, incluye `README.md` y todos los recursos necesarios para la ruta de ejecución documentada.
- [ ] El ZIP no incluye credenciales, `.env`, `.venv`, `.git`, cachés ni artefactos temporales.
- [ ] La ejecución local documentada funciona al seguir el README desde una copia extraída.
- [ ] El DOCX y el ZIP se abren/revisan una última vez y tienen nombres claros para subir a la actividad.
- [ ] Los cinco diagramas SVG se ven correctamente en el DOCX; si el editor no soporta SVG, insertar exportaciones PNG de alta resolución con el mismo contenido.

## Datos que debe confirmar el grupo

- Universidad e integrantes con nombres completos.
- Ciudad y fecha que deben figurar en portada.
- Si el docente exige un estilo bibliográfico específico (por ejemplo, APA 7).
- Si las 15 capturas deben ser todas explícitamente de VS Code; este plan adopta esa interpretación estricta para evitar incumplir la consigna.
