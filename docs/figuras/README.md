# Diagramas del informe

Los diagramas están en SVG para conservar la nitidez al ampliarlos y permitir ajustes futuros. Se enlazan desde `docs/BORRADOR_INFORME.md` y se muestran en `docs/BORRADOR_INFORME.html`.

| Archivo | Contenido | Figura en el informe |
|---|---|---:|
| `01-arquitectura-proyecto.svg` | Componentes ejecutados localmente y servicios configurados/no ejecutados | 1 |
| `02-flujo-etl.svg` | Ingreso sintético y transformación Bronze/Silver/Gold con rechazo | 27 |
| `03-secuencia-drp.svg` | Secuencia de sincronización, failover, validación y restauración del simulacro | 28 |
| `04-estados-circuit-breaker.svg` | Estados y transiciones del patrón Circuit Breaker | 29 |
| `05-comparacion-sharding.svg` | Distribución hash de línea base y después de priorizar `cliente_id` | 30 |

Los resultados del diagrama de sharding son dos corridas separadas con datos sintéticos; no constituyen una garantía estadística. La arquitectura también identifica como configurados pero no levantados los servicios externos que no participaron en las pruebas.

Para la entrega, conservar estos SVG junto al documento fuente. Si la versión de Word/LibreOffice usada no admite SVG, insertar una exportación PNG de alta resolución del mismo diagrama, sin editar los datos ni las etiquetas.
