#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
# inicializar.sh
# Script de inicialización del entorno completo BigData
#
# Uso:
#   chmod +x scripts/inicializar.sh
#   ./scripts/inicializar.sh [--solo-python] [--solo-docker] [--ayuda]
# ══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ── Colores ────────────────────────────────────────────────────────────────────
VERDE="\033[0;32m"
AMARILLO="\033[0;33m"
ROJO="\033[0;31m"
AZUL="\033[0;34m"
RESET="\033[0m"

ok()    { echo -e "${VERDE}  ✅ $*${RESET}"; }
info()  { echo -e "${AZUL}  ℹ  $*${RESET}"; }
warn()  { echo -e "${AMARILLO}  ⚠  $*${RESET}"; }
error() { echo -e "${ROJO}  ❌ $*${RESET}"; exit 1; }
title() { echo -e "\n${VERDE}══════════════════════════════════════════════════${RESET}"; \
          echo -e "${VERDE}  $*${RESET}"; \
          echo -e "${VERDE}══════════════════════════════════════════════════${RESET}"; }

# ── Parámetros ─────────────────────────────────────────────────────────────────
SOLO_PYTHON=false
SOLO_DOCKER=false
SKIP_DOCKER=false

for arg in "$@"; do
  case $arg in
    --solo-python)  SOLO_PYTHON=true ;;
    --solo-docker)  SOLO_DOCKER=true ;;
    --sin-docker)   SKIP_DOCKER=true ;;
    --ayuda|-h)
      echo ""
      echo "  Uso: $0 [opciones]"
      echo ""
      echo "  Opciones:"
      echo "    --solo-python   Instala solo dependencias Python (sin Docker)"
      echo "    --solo-docker   Solo levanta los servicios Docker"
      echo "    --sin-docker    Configura Python, omite Docker"
      echo "    --ayuda         Muestra esta ayuda"
      echo ""
      exit 0 ;;
  esac
done

# ── Banner ─────────────────────────────────────────────────────────────────────
clear
echo -e "${VERDE}"
echo "  ╔══════════════════════════════════════════════════════════╗"
echo "  ║   INICIALIZACIÓN — BigData Arquitecturas Robustas       ║"
echo "  ║   Maestría en Big Data y Analítica de Datos 2026        ║"
echo "  ╚══════════════════════════════════════════════════════════╝"
echo -e "${RESET}"

# ══════════════════════════════════════════════════════════════════════════════
# PASO 1 — Verificar prerequisitos del sistema
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SOLO_DOCKER" = false ]; then
  title "PASO 1 · Verificando prerequisitos del sistema"

  # Python 3.10+
  if command -v python3 &>/dev/null; then
    PYTHON_VER=$(python3 --version | awk '{print $2}')
    PYTHON_MAJOR=$(echo "$PYTHON_VER" | cut -d. -f1)
    PYTHON_MINOR=$(echo "$PYTHON_VER" | cut -d. -f2)
    if [ "$PYTHON_MAJOR" -ge 3 ] && [ "$PYTHON_MINOR" -ge 10 ]; then
      ok "Python $PYTHON_VER detectado"
    else
      error "Se requiere Python 3.10+. Detectado: $PYTHON_VER"
    fi
  else
    error "Python 3 no encontrado. Instalar desde https://www.python.org"
  fi

  # pip
  if command -v pip3 &>/dev/null || command -v pip &>/dev/null; then
    ok "pip detectado"
  else
    error "pip no encontrado. Ejecutar: python3 -m ensurepip --upgrade"
  fi

  # Java (para PySpark)
  if command -v java &>/dev/null; then
    JAVA_VER=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}')
    ok "Java $JAVA_VER detectado (necesario para PySpark)"
  else
    warn "Java no detectado. PySpark no funcionará. Instalar OpenJDK 11+."
    warn "  Ubuntu: sudo apt-get install openjdk-11-jdk"
    warn "  macOS:  brew install openjdk@11"
  fi
fi

# Docker y Docker Compose
if [ "$SKIP_DOCKER" = false ] && [ "$SOLO_PYTHON" = false ]; then
  if command -v docker &>/dev/null; then
    DOCKER_VER=$(docker --version | awk '{print $3}' | tr -d ',')
    ok "Docker $DOCKER_VER detectado"
  else
    warn "Docker no detectado. El stack completo no estará disponible."
    warn "  Instalar desde https://docs.docker.com/get-docker/"
    SKIP_DOCKER=true
  fi

  if command -v docker &>/dev/null && docker compose version &>/dev/null 2>&1; then
    ok "Docker Compose V2 detectado"
  elif command -v docker-compose &>/dev/null; then
    ok "Docker Compose V1 detectado"
  else
    warn "Docker Compose no detectado. Instalar junto con Docker Desktop."
    SKIP_DOCKER=true
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# PASO 2 — Configurar variables de entorno
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SOLO_DOCKER" = false ]; then
  title "PASO 2 · Configurando variables de entorno"

  if [ ! -f ".env" ]; then
    cp .env.example .env
    ok "Archivo .env creado desde .env.example"
    info "Edita .env para personalizar la configuración antes de continuar."
  else
    ok "Archivo .env ya existe (no se sobreescribe)"
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# PASO 3 — Entorno virtual Python e instalación de dependencias
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SOLO_DOCKER" = false ]; then
  title "PASO 3 · Instalando dependencias Python"

  # Crear entorno virtual si no existe
  if [ ! -d ".venv" ]; then
    info "Creando entorno virtual en .venv/ ..."
    python3 -m venv .venv
    ok "Entorno virtual creado"
  else
    ok "Entorno virtual .venv/ ya existe"
  fi

  # Activar entorno
  # shellcheck disable=SC1091
  source .venv/bin/activate
  ok "Entorno virtual activado"

  # Actualizar pip
  pip install --upgrade pip --quiet
  ok "pip actualizado"

  # Instalar dependencias base (sin Spark ni Airflow para instalación rápida)
  info "Instalando dependencias base (puede tardar 2-5 minutos)..."
  pip install \
    pandas numpy pyarrow \
    kafka-python \
    boto3 \
    psycopg2-binary sqlalchemy \
    streamlit plotly \
    python-dotenv pydantic tenacity rich \
    pytest faker \
    --quiet

  ok "Dependencias base instaladas"

  # PySpark (opcional, requiere Java)
  if command -v java &>/dev/null; then
    info "Instalando PySpark (requiere ~500MB)..."
    pip install pyspark==3.5.1 --quiet && ok "PySpark instalado" || warn "PySpark falló. Continuar sin él."
  else
    warn "Omitiendo PySpark (Java no disponible)"
  fi

  # Delta Lake (opcional)
  if pip show pyspark &>/dev/null 2>&1; then
    pip install delta-spark==3.2.0 --quiet && ok "Delta Lake instalado" || warn "Delta Lake no instalado."
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# PASO 4 — Crear directorios necesarios
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SOLO_DOCKER" = false ]; then
  title "PASO 4 · Creando estructura de directorios"
  mkdir -p datos/lakehouse/{raw,silver,gold}
  mkdir -p datos/backups
  mkdir -p logs
  ok "Directorios creados: datos/lakehouse/{raw,silver,gold}, logs/"
fi

# ══════════════════════════════════════════════════════════════════════════════
# PASO 5 — Generar datos de muestra
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SOLO_DOCKER" = false ]; then
  title "PASO 5 · Generando datos de muestra"
  if source .venv/bin/activate 2>/dev/null || true; then
    python3 -c "
import sys; sys.path.insert(0, '.')
from src.ingesta.simulador_transacciones import guardar_muestra_json
guardar_muestra_json('datos/muestra_transacciones.json', n=100)
print('  Muestra generada: datos/muestra_transacciones.json (100 transacciones)')
" && ok "Datos de muestra generados" || warn "No se pudieron generar datos de muestra"
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# PASO 6 — Levantar servicios Docker
# ══════════════════════════════════════════════════════════════════════════════
if [ "$SKIP_DOCKER" = false ] && [ "$SOLO_PYTHON" = false ]; then
  title "PASO 6 · Levantando servicios Docker"

  info "Descargando imágenes Docker (primera vez puede tardar ~10 minutos)..."
  if docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
  else
    COMPOSE_CMD="docker-compose"
  fi

  $COMPOSE_CMD up -d --remove-orphans

  info "Esperando que los servicios estén listos (30s)..."
  sleep 30

  # Verificar servicios
  for servicio in bigdata-kafka bigdata-postgres-dw bigdata-seaweedfs; do
    if docker ps --format '{{.Names}}' | grep -q "^${servicio}$"; then
      ok "Servicio '$servicio' activo"
    else
      warn "Servicio '$servicio' no encontrado. Verificar con: docker compose ps"
    fi
  done
fi

# ══════════════════════════════════════════════════════════════════════════════
# RESUMEN FINAL
# ══════════════════════════════════════════════════════════════════════════════
title "INICIALIZACIÓN COMPLETADA"

echo ""
echo -e "${AZUL}  Próximos pasos:${RESET}"
echo ""
echo "  1. Ejecutar la demo standalone (sin Docker):"
echo -e "     ${VERDE}source .venv/bin/activate${RESET}"
echo -e "     ${VERDE}python demo.py${RESET}"
echo ""
echo "  2. Demo de un bloque específico:"
echo -e "     ${VERDE}python demo.py --bloque 3${RESET}  # Alta Disponibilidad"
echo ""
echo "  3. Lanzar el dashboard Streamlit:"
echo -e "     ${VERDE}streamlit run dashboard/app_streamlit.py${RESET}"
echo ""
if [ "$SKIP_DOCKER" = false ]; then
  echo "  4. Acceder a las interfaces web:"
  echo -e "     ${AZUL}Kafka UI:    ${RESET}http://localhost:8080"
  echo -e "     ${AZUL}SeaweedFS:   ${RESET}http://localhost:8888  (Filer UI) | S3 API: http://localhost:8333 (lakeadmin / lakeadmin123)"
  echo -e "     ${AZUL}Airflow:     ${RESET}http://localhost:8081  (admin / admin2026)"
  echo -e "     ${AZUL}PgAdmin:     ${RESET}http://localhost:5050  (admin@bigdata.bo / admin2026)"
  echo ""
  echo "  5. Detener todos los servicios:"
  echo -e "     ${VERDE}docker compose down${RESET}"
  echo ""
fi
echo -e "  ${VERDE}Documentación completa: README.md${RESET}"
echo ""
