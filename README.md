# Diseño de Arquitecturas para Sistemas de Big Data

> **Maestría en Big Data y Analítica de Datos · 2026**  
> **Docente:** MSc. Ing. Marcelo Fernando Condori Mendoza  
> **Módulo:** Contenidos y Tendencias Tecnológicas

---

## Descripción del Proyecto

Proyecto académico aplicado que implementa los cinco bloques del módulo utilizando un **sistema de monitoreo de transacciones financieras** como caso de uso. Demuestra de forma práctica los conceptos de escalabilidad, alta disponibilidad, recuperación ante desastres, replicación y particionamiento en arquitecturas Big Data reales.

```
Clase02 · Diseño de Arquitecturas para Sistemas de Big Data
Escalabilidad · Disponibilidad · DRP · Replicación · Particionamiento
```

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     ARQUITECTURA LAMBDA / KAPPA                         │
├──────────────┬──────────────────────────────────────┬───────────────────┤
│  CAPA DE     │   Simulador de Transacciones          │                   │
│  INGESTA     │   ──────────────────────────          │  CAPA DE SERVICIO │
│              │   Productor Kafka  →  Tópicos Kafka  │                   │
│  (Velocidad) │   Consumer Groups + Circuit Breaker  │  Dashboard        │
├──────────────┴──────────────────────────────────────┤  Streamlit        │
│  CAPA BATCH  │  PySpark ETL: Bronze → Silver → Gold │                   │
│  (Precisión) │  Apache Airflow DAG (orquestación)   │  Airflow UI       │
├──────────────┼─────────────────────────────────────┤                   │
│  ALMACENA-   │  Data Lake   │ Data Warehouse │ Lake-│  Kafka UI         │
│  MIENTO      │ SeaweedFS(S3)│ PostgreSQL     │ house│                   │
│              │  zona-raw    │ gold.*         │ Delta│  PgAdmin          │
├──────────────┴─────────────────────────────────────┴───────────────────┤
│  RESILIENCIA  Circuit Breaker · Sharding · Replicación · DRP/RTO/RPO   │
│  & MONITOREO  Métricas · Logs · Trazas Distribuidas · Alertas          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Estructura del Proyecto

```
proyecto-bigdata/
│
├── README.md                          ← Este archivo
├── demo.py                            ← Demo standalone (sin Docker)
├── docker-compose.yml                 ← Stack completo de servicios
├── requirements.txt                   ← Dependencias Python
├── .env.example                       ← Plantilla de variables de entorno
│
├── src/                               ← Código fuente principal
│   ├── config.py                      ← Configuración centralizada
│   │
│   ├── ingesta/                       ── Bloque 1 & 2: Ingesta de datos
│   │   ├── simulador_transacciones.py ← Generador de datos (las 5 V's)
│   │   ├── productor_kafka.py         ← Productor Kafka con Circuit Breaker
│   │   └── consumidor_kafka.py        ← Consumidor con checkpointing (RPO)
│   │
│   ├── procesamiento/                 ── Bloque 2: Motores de procesamiento
│   │   └── motor_batch.py             ← Pipeline ETL Bronze→Silver→Gold
│   │
│   ├── almacenamiento/                ── Bloque 2: Capas de almacenamiento
│   │
│   ├── resiliencia/                   ── Bloque 3 & 5: Alta disponibilidad
│   │   ├── circuit_breaker.py         ← Patrón Circuit Breaker
│   │   ├── particionamiento.py        ← Sharding horizontal y vertical
│   │   └── replicacion.py             ← Síncrona / Asíncrona / Multi-líder
│   │
│   ├── recuperacion/                  ── Bloque 4: DRP y BCP
│   │   └── plan_drp.py                ← DRP completo con RTO/RPO y simulacros
│   │
│   └── monitoreo/                     ── Bloque 3: Observabilidad
│       └── observabilidad.py          ← Métricas, logs, trazas y alertas
│
├── dashboard/
│   └── app_streamlit.py               ← Dashboard interactivo (Streamlit)
│
├── dags/
│   └── pipeline_arquitectura.py       ← DAG Airflow (orquestación)
│
├── scripts/
│   ├── inicializar.sh                 ← Script de setup del entorno
│   └── init_warehouse.sql             ← DDL del Data Warehouse (Star Schema)
│
└── datos/
    └── lakehouse/
        ├── raw/                       ← Bronze: datos crudos
        ├── silver/                    ← Silver: datos limpios
        └── gold/                      ← Gold: KPIs y agregaciones
```

---

## Mapa de Conceptos a Módulos

| Bloque del módulo | Concepto | Archivo(s) implementados |
|---|---|---|
| **Bloque 1** · Soluciones Escalables | Las 5 V's, Escalado horizontal, Lambda/Kappa | `simulador_transacciones.py`, `motor_batch.py` |
| **Bloque 1** · Cloud-native | Contenedores, Autoescalado, IaC | `docker-compose.yml`, `scripts/inicializar.sh` |
| **Bloque 2** · Tecnologías | Batch vs Streaming, Lakehouse, Orquestación | `motor_batch.py`, `pipeline_arquitectura.py` |
| **Bloque 2** · Almacenamiento | Data Lake / Warehouse / Lakehouse | `init_warehouse.sql`, `docker-compose.yml` |
| **Bloque 3** · Alta Disponibilidad | Circuit Breaker, Failover, Multi-AZ | `circuit_breaker.py`, `plan_drp.py` |
| **Bloque 3** · Observabilidad | Métricas, Logs, Trazas, Alertas | `observabilidad.py`, `app_streamlit.py` |
| **Bloque 4** · DRP / BCP | RTO, RPO, Sitios Hot/Warm/Cold | `plan_drp.py` |
| **Bloque 4** · Multi-región | Replicación entre regiones, DNS/anycast | `replicacion.py`, `plan_drp.py` |
| **Bloque 5** · Particionamiento | Sharding hash/geográfico/rango | `particionamiento.py` |
| **Bloque 5** · Replicación | Síncrona, Asíncrona, Multi-líder, CAP | `replicacion.py` |

---

## Prerequisitos

### 1. Sistema Operativo

Compatible con **Linux**, **macOS** y **Windows** (con WSL2 recomendado).

### 2. Python 3.10 o superior

```bash
# Verificar versión instalada
python3 --version   # Debe ser >= 3.10

# Si no está instalado:
# Ubuntu / Debian
sudo apt-get update && sudo apt-get install python3.11 python3.11-venv python3-pip

# macOS (con Homebrew)
brew install python@3.11

# Windows — descargar desde https://www.python.org/downloads/
```

### 3. Java 11+ *(solo para PySpark — opcional)*

PySpark requiere Java para ejecutarse. Si no se instala, el motor batch funcionará en modo simulación con pandas.

```bash
# Ubuntu / Debian
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# macOS
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Verificar
java -version   # Debe mostrar openjdk version "11" o superior
```

### 4. Docker Desktop *(para el stack completo — opcional)*

Necesario para ejecutar Kafka, PostgreSQL, SeaweedFS y Airflow.

- **Linux:** https://docs.docker.com/engine/install/
- **macOS / Windows:** https://www.docker.com/products/docker-desktop/

Recursos mínimos recomendados para Docker:
- CPU: 4 núcleos
- RAM: 8 GB asignados a Docker
- Disco: 15 GB libres

```bash
# Verificar instalación
docker --version           # >= 24.0
docker compose version     # >= 2.20
```

### 5. Git

```bash
git --version   # Cualquier versión reciente
```

---

## Instalación

### Opción A — Instalación Automática (recomendada)

```bash
# 1. Clonar o posicionarse en el directorio del proyecto
cd proyecto-bigdata

# 2. Dar permisos al script
chmod +x scripts/inicializar.sh

# 3. Inicializar con Docker (stack completo)
./scripts/inicializar.sh

# 3b. Inicializar solo Python (sin Docker)
./scripts/inicializar.sh --solo-python
```

El script realizará automáticamente:
1. Verificación de prerequisitos
2. Creación de variables de entorno (`.env`)
3. Entorno virtual Python + instalación de dependencias
4. Creación de directorios del Lakehouse
5. Generación de datos de muestra
6. Levantamiento de servicios Docker

---

### Opción B — Instalación Manual Paso a Paso

#### Paso 1: Clonar y posicionarse

```bash
cd proyecto-bigdata
```

#### Paso 2: Crear el archivo de variables de entorno

```bash
cp .env.example .env
# Editar si necesario
nano .env   # o code .env / vim .env
```

#### Paso 3: Crear entorno virtual Python

```bash
python3 -m venv .venv

# Activar entorno
source .venv/bin/activate           # Linux / macOS
# ó
.venv\Scripts\activate              # Windows PowerShell
```

#### Paso 4: Instalar dependencias

```bash
# Dependencias base (rápido, ~2 min)
pip install pandas kafka-python boto3 psycopg2-binary sqlalchemy \
            streamlit plotly python-dotenv pydantic tenacity rich \
            pytest faker

# Dependencias completas (incluye PySpark y Airflow, ~10 min)
pip install -r requirements.txt
```

#### Paso 5: Crear estructura de datos

```bash
mkdir -p datos/lakehouse/{raw,silver,gold} datos/backups logs
```

#### Paso 6: Generar datos de muestra

```bash
python3 -c "
from src.ingesta.simulador_transacciones import guardar_muestra_json
guardar_muestra_json('datos/muestra_transacciones.json', n=200)
print('Muestra creada.')
"
```

#### Paso 7: Levantar servicios Docker *(opcional)*

```bash
# Levantar todo el stack
docker compose up -d

# Ver estado de los servicios
docker compose ps

# Seguir los logs
docker compose logs -f kafka
```

---

## Ejecución

### Demo Standalone (sin Docker requerido)

La forma más rápida de ejecutar el proyecto completo:

```bash
# Activar entorno virtual (si no está activo)
source .venv/bin/activate

# Ejecutar los 5 bloques del módulo
python demo.py

# Ejecutar un bloque específico
python demo.py --bloque 1    # Soluciones Escalables
python demo.py --bloque 2    # Selección de Tecnologías
python demo.py --bloque 3    # Alta Disponibilidad
python demo.py --bloque 4    # Recuperación ante Desastres
python demo.py --bloque 5    # Replicación y Particionamiento

# Modo verbose (muestra logs internos)
python demo.py --completo
```

**Salida esperada — ejemplo Bloque 3:**
```
════════════════════════════════════════════════════
  BLOQUE 3 · ALTA DISPONIBILIDAD
  Circuit Breaker · Tolerancia a Fallos · Observabilidad
════════════════════════════════════════════════════

  ─── Circuit Breaker — Patrón de Resiliencia ──────────

  [01]  🟢 CERRADO         ✅ Éxito (47ms)           Llamada procesada
  [02]  🟢 CERRADO         ❌ Error de conexión        Fallo registrado
  [03]  🟢 CERRADO         ❌ Error de conexión        Fallo registrado
  [04]  🟡 CERRADO         ❌ Error de conexión        Fallo registrado
  [05]  🔴 ABIERTO         🚫 RECHAZADO (sin llamar)   Fallo en cascada evitado
  ...
  ✅ Tasa de éxito:                          35.0%
  ✅ Rechazos (CB abierto):                  4
```

---

### Dashboard Interactivo (Streamlit)

```bash
# Asegurarse de que el entorno virtual está activo
source .venv/bin/activate

# Lanzar el dashboard
streamlit run dashboard/app_streamlit.py

# El dashboard se abrirá automáticamente en:
# http://localhost:8501
```

El dashboard incluye:
- **Bloque 1:** Ejecución del pipeline ETL con visualizaciones (gráficos por tipo y región)
- **Bloque 2:** Comparación de capas de almacenamiento
- **Bloque 3:** Simulación interactiva del Circuit Breaker
- **Bloque 4:** Simulacro DRP con métricas RTO/RPO en tiempo real
- **Bloque 5:** Visualización de distribución de shards

---

### Módulos Individualmente

Ejecuta estos comandos desde la raíz del proyecto con el entorno virtual activado. El módulo DRP se invoca como módulo para que Python resuelva correctamente el paquete `src`:

```bash
# Simular transacciones
python src/ingesta/simulador_transacciones.py

# Probar el Circuit Breaker
python src/resiliencia/circuit_breaker.py

# Probar estrategias de particionamiento
python src/resiliencia/particionamiento.py

# Probar estrategias de replicación y comparar latencias
python src/resiliencia/replicacion.py

# Ejecutar pipeline ETL batch
python src/procesamiento/motor_batch.py

# Simular DRP completo con simulacro
python -m src.recuperacion.plan_drp

# Probar el sistema de observabilidad
python src/monitoreo/observabilidad.py
```

---

### Con Docker (stack completo)

Una vez levantado el stack con `docker compose up -d`:

#### Productor Kafka (streaming)

```bash
# Desde el entorno virtual local (conecta a Kafka en Docker)
source .venv/bin/activate
python src/ingesta/productor_kafka.py
```

#### Consumidor Kafka

```bash
source .venv/bin/activate
python src/ingesta/consumidor_kafka.py
```

> **Nota — MinIO reemplazado por SeaweedFS:** MinIO retiró sus imágenes de Docker Hub
> (`pull access denied for minio/minio`). El Data Lake usa ahora SeaweedFS, que expone una
> API S3 compatible. Copia de nuevo `.env.example` a `.env` (variables `S3_*` en lugar de `MINIO_*`).
> Si ya habías levantado el stack antes: `docker compose down --remove-orphans`.

#### Interfaces web disponibles

| Servicio | URL | Credenciales |
|---|---|---|
| **Kafka UI** (monitoreo de tópicos) | http://localhost:8080 | Sin autenticación |
| **SeaweedFS Filer UI** (Data Lake, buckets en `/buckets`) | http://localhost:8888 | Sin autenticación |
| **SeaweedFS S3 API** (boto3 / aws cli) | http://localhost:8333 | lakeadmin / lakeadmin123 |
| **Apache Airflow** (orquestación) | http://localhost:8081 | admin / admin2026 |
| **PgAdmin** (Data Warehouse) | http://localhost:5050 | admin@bigdata.bo / admin2026 |

#### Conexión a PostgreSQL (Data Warehouse)

```
Host:     localhost
Puerto:   5432
Base de datos: bigdata_dw
Usuario:  bigdata
Contraseña: bigdata2026
```

---

### Tests

```bash
source .venv/bin/activate

# Ejecutar todos los tests
pytest tests/ -v

# Test con cobertura
pytest tests/ --cov=src --cov-report=html

# Test de un módulo específico
pytest tests/test_circuit_breaker.py -v
```

---

## Detener y Limpiar

```bash
# Detener servicios (preserva los datos en volúmenes)
docker compose down

# Detener y eliminar TODOS los datos (volúmenes incluidos)
docker compose down -v

# Eliminar imágenes descargadas
docker compose down -v --rmi all

# Limpiar datos locales del lakehouse
rm -rf datos/lakehouse/{raw,silver,gold}/*
```

---

## Conceptos Implementados por Bloque

### Bloque 1 · Soluciones Escalables

| Concepto | Implementación |
|---|---|
| Las 5 V's del Big Data | `simulador_transacciones.py` — genera datos con Volumen, Velocidad, Variedad, Veracidad y Valor |
| Escalado horizontal | Múltiples particiones Kafka + sharding en 3 nodos |
| Desacoplamiento de componentes | Productor → Kafka → Consumidor → ETL (independientes) |
| Elasticidad | Docker Compose permite escalar réplicas: `docker compose up --scale kafka=3` |
| Arquitectura Lambda | Capa batch (`motor_batch.py`) + Capa velocidad (`productor_kafka.py` + `consumidor_kafka.py`) |
| Arquitectura Kappa | Pipeline único de streaming con Kafka como backbone |
| Data Mesh / Lakehouse | Zonas Bronze/Silver/Gold en SeaweedFS (S3) con formato Parquet / Delta Lake |

### Bloque 2 · Selección de Tecnologías

| Concepto | Implementación |
|---|---|
| Batch (Apache Spark) | `motor_batch.py` — ETL con PySpark o pandas |
| Streaming (Apache Kafka) | `productor_kafka.py` y `consumidor_kafka.py` |
| Data Lake | SeaweedFS (zona-raw) — datos crudos en cualquier formato |
| Data Warehouse | PostgreSQL con Star Schema (`init_warehouse.sql`) |
| Data Lakehouse | Delta Lake sobre SeaweedFS — ACID + formatos abiertos |
| Orquestación | Apache Airflow DAG (`pipeline_arquitectura.py`) |
| Gobernanza | Linaje registrado en `control.linaje_pipeline` |
| Transformación gobernada | Pipeline versionado Bronze → Silver → Gold |

### Bloque 3 · Alta Disponibilidad

| Concepto | Implementación |
|---|---|
| Circuit Breaker | `circuit_breaker.py` — estados CERRADO/ABIERTO/SEMI-ABIERTO |
| Redundancia de componentes | Kafka con múltiples particiones, PostgreSQL con pool de conexiones |
| Replicación de datos | `replicacion.py` — sincronización en nodos activos y pasivos |
| Checkpointing | `consumidor_kafka.py` — commit manual de offsets post-procesamiento |
| Auto-recuperación | `consumidor_kafka.py` — reconexión automática ante fallos Kafka |
| Failover automático | `plan_drp.py` — activación automática del sitio standby |
| Multi-AZ / Multi-región | `docker-compose.yml` — zonas de disponibilidad simuladas |
| Observabilidad | `observabilidad.py` — Métricas, Logs estructurados, Trazas, Alertas |

### Bloque 4 · Recuperación ante Desastres

| Concepto | Implementación |
|---|---|
| Plan DRP | `plan_drp.py` — runbook completo automatizado |
| Análisis de Impacto (BIA) | Lista de procesos con prioridad, RTO y RPO individuales |
| Sitio Hot (< 1 min failover) | `SitioRespaldo(tipo=TipoSitio.HOT)` en `plan_drp.py` |
| Sitio Warm (5-15 min) | `SitioRespaldo(tipo=TipoSitio.WARM)` — sincronización semi-continua |
| Sitio Cold (> 1 hora) | `SitioRespaldo(tipo=TipoSitio.COLD)` — backups periódicos |
| RTO (Recovery Time Objective) | Medido y comparado con objetivo en cada simulacro |
| RPO (Recovery Point Objective) | Calculado desde el último backup exitoso |
| Simulacros periódicos | `drp.ejecutar_simulacro()` — valida RTO/RPO reales |
| BCP (Business Continuity Plan) | Procesos críticos ordenados por prioridad y tolerancia |
| Multi-región | Arquitectura Región A (activa) → B (standby) → C (cold) |

### Bloque 5 · Replicación y Particionamiento

| Concepto | Implementación |
|---|---|
| Sharding horizontal | `GestorParticionamiento` — distribuye filas por clave de partición |
| Hash consistente | `ShardHashing` — nodos virtuales para distribución uniforme |
| Particionamiento por rango | `ShardPorRango` — útil para series temporales |
| Particionamiento geográfico | `ShardGeografico` — datos cerca del usuario (latencia + compliance) |
| Particionamiento vertical | `particionar_verticalmente()` — columnas hot vs. cold |
| Detección de hotspots | `estadisticas()` — alerta si balance < 70% |
| Replicación síncrona | `ReplicacionSincrona` — acks=all, máxima consistencia (CP) |
| Replicación asíncrona | `ReplicacionAsincrona` — menor latencia, riesgo pérdida (AP) |
| Multi-líder (multi-master) | `ReplicacionMultiLider` — escritura en múltiples regiones |
| Resolución de conflictos | Last-Write-Wins (LWW) por timestamp en multi-líder |
| Teorema CAP | Demostrado comparativamente con latencias medidas |

---

## Solución de Problemas Frecuentes

### `ModuleNotFoundError: No module named 'src'`

```bash
# Asegurarse de ejecutar desde la raíz del proyecto
cd /ruta/completa/proyecto-bigdata
python demo.py

# O establecer el PYTHONPATH
export PYTHONPATH=$(pwd)
python demo.py
```

### `NoBrokersAvailable` al usar módulos Kafka

Este error ocurre cuando Kafka no está corriendo. Opciones:

```bash
# Opción 1: levantar Docker
docker compose up -d kafka zookeeper

# Opción 2: usar la demo standalone (no requiere Kafka)
python demo.py
```

### Error de conexión a PostgreSQL

```bash
# Verificar que el contenedor esté corriendo
docker compose ps postgres-dw

# Reiniciar si es necesario
docker compose restart postgres-dw

# Ver logs
docker compose logs postgres-dw
```

### Dashboard Streamlit no abre

```bash
# Verificar que streamlit está instalado
pip show streamlit

# Reinstalar si es necesario
pip install streamlit plotly

# Ejecutar con puerto diferente si el 8501 está ocupado
streamlit run dashboard/app_streamlit.py --server.port 8502
```

### PySpark falla con `JAVA_HOME not found`

```bash
# Ubuntu / Debian
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# macOS
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Agregar al archivo de perfil para que sea permanente
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### `docker compose` vs `docker-compose`

```bash
# Docker Compose V2 (recomendado)
docker compose up -d

# Docker Compose V1 (legacy)
docker-compose up -d
```

### Memoria insuficiente para Docker

Reducir los servicios levantados al mínimo necesario:

```bash
# Solo Kafka y Zookeeper
docker compose up -d zookeeper kafka

# Solo PostgreSQL
docker compose up -d postgres-dw

# Solo el Data Lake (SeaweedFS)
docker compose up -d seaweedfs seaweedfs-init
```

---

## Tecnologías Utilizadas

| Tecnología | Versión | Rol en la arquitectura |
|---|---|---|
| **Python** | 3.10+ | Lenguaje principal |
| **Apache Kafka** | 7.6.1 | Capa de velocidad (streaming) |
| **Apache Spark (PySpark)** | 3.5.1 | Motor de procesamiento batch |
| **Delta Lake** | 3.2.0 | Formato Lakehouse (ACID sobre object storage) |
| **PostgreSQL** | 16 | Data Warehouse (Star Schema) |
| **SeaweedFS** | latest | Data Lake (Object Storage S3-compatible; reemplaza a MinIO) |
| **Apache Airflow** | 2.9.3 | Orquestación de pipelines |
| **Streamlit** | 1.36.0 | Dashboard de monitoreo |
| **Plotly** | 5.22.0 | Visualizaciones interactivas |
| **Docker Compose** | V2 | Contenedores y orquestación de servicios |
| **pandas** | 2.2.2 | Procesamiento de datos en memoria |
| **pyarrow** | 16.1.0 | Serialización columnar (Parquet) |
| **kafka-python** | 2.0.2 | Cliente Kafka para Python |
| **SQLAlchemy** | 2.0.30 | ORM para PostgreSQL |
| **Pydantic** | 2.7.4 | Validación de datos tipados |

---

## Glosario

| Término | Definición |
|---|---|
| **RTO** | *Recovery Time Objective* — Tiempo máximo tolerable para restaurar el servicio tras un incidente |
| **RPO** | *Recovery Point Objective* — Máxima cantidad de datos (en tiempo) que la organización puede permitirse perder |
| **DRP** | *Disaster Recovery Plan* — Plan documentado para restaurar sistemas críticos tras un desastre |
| **BCP** | *Business Continuity Plan* — Plan más amplio que asegura la continuidad operativa del negocio |
| **BIA** | *Business Impact Analysis* — Análisis que identifica procesos críticos y su tolerancia a la interrupción |
| **HA** | *High Availability* — Alta disponibilidad (objetivo: 99.99% = "4 nueves" ≈ 52 min caída/año) |
| **Circuit Breaker** | Patrón que detiene llamadas a servicios degradados para evitar fallos en cascada |
| **Sharding** | Particionamiento horizontal de datos entre múltiples nodos |
| **LWW** | *Last Write Wins* — Estrategia de resolución de conflictos en replicación multi-líder |
| **Lakehouse** | Arquitectura que combina la flexibilidad del Data Lake con la consistencia del Data Warehouse |
| **ETL** | *Extract, Transform, Load* — Proceso de extracción, transformación y carga de datos |
| **DAG** | *Directed Acyclic Graph* — Grafo acíclico dirigido que modela las dependencias de un pipeline |
| **CAP** | Teorema que establece que un sistema distribuido solo puede garantizar 2 de: Consistencia, Disponibilidad, Tolerancia a Particiones |
| **SLA** | *Service Level Agreement* — Acuerdo de nivel de servicio que define disponibilidad y tiempos de respuesta |

---

## Referencias

- Nathan Marz & James Warren — *Big Data: Principles and Best Practices of Scalable Real-Time Data Systems* (Manning, 2015)
- Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly, 2017)
- Apache Kafka Documentation — https://kafka.apache.org/documentation/
- Apache Spark Documentation — https://spark.apache.org/docs/latest/
- Delta Lake Documentation — https://docs.delta.io/
- Apache Airflow Documentation — https://airflow.apache.org/docs/
- Databricks — *The Data Lakehouse Platform* — https://www.databricks.com/glossary/data-lakehouse

---

*Proyecto académico — Maestría en Big Data y Analítica de Datos · 2026*  
*Docente: MSc. Ing. Marcelo Fernando Condori Mendoza*
