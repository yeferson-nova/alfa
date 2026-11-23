#!/usr/bin/env bash
# Diagnóstico de SOLO LECTURA de la malla Emisiones (Generales, Satélites y PCR Vida).
# Uso en CloudShell, con este archivo y diagnostico_pcr_emisiones.py en la misma carpeta:
#   bash diagnostico-pcr-emisiones.sh <ambiente> [periodo] [workgroup_athena]
#   bash diagnostico-pcr-emisiones.sh qa 202606 primary
#   bash diagnostico-pcr-emisiones.sh prd 202606 primary
# Deja evidencia-pcr-emisiones-<ambiente>-<fecha>.tar.gz para descargar (Acciones > Descargar archivo).
set -euo pipefail
export AWS_PAGER=""
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1

AMB="${1:-}"
PERIODO="${2:-202606}"
WG="${3:-primary}"
case "$AMB" in
  dev|qa|prd|prod) ;;
  *) echo "Uso: bash $0 {dev|qa|prd} [periodo AAAAMM] [workgroup de Athena]"; exit 1 ;;
esac

python3 -c "import boto3" 2>/dev/null || pip3 install --user --quiet boto3
DIR="$(cd "$(dirname "$0")" && pwd)"
# Para repetir solo una parte: SECCIONES=configuracion,datos bash diagnostico-pcr-emisiones.sh qa
SECCIONES="${SECCIONES:-codigo,orquestacion,configuracion,fuente,almacenamiento,datos,oracle}"
python3 "$DIR/diagnostico_pcr_emisiones.py" --amb "$AMB" --periodo "$PERIODO" --workgroup "$WG" --secciones "$SECCIONES"
