#!/usr/bin/env bash
# Diagnóstico de SOLO LECTURA de la malla Emisiones (Generales, Satélites y PCR Vida).
# Uso en CloudShell, con este archivo y diag_emisiones_v2.py en la misma carpeta:
#   bash diag-emisiones-v2.sh <ambiente> [periodo] [workgroup_athena]
#   bash diag-emisiones-v2.sh qa 202606 primary
#   bash diag-emisiones-v2.sh prd 202606 primary
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
PY="$DIR/diag_emisiones_v2.py"
VERSION_ESPERADA="2026-11-24.3"
if ! grep -q "VERSION = \"$VERSION_ESPERADA" "$PY" 2>/dev/null; then
  echo "ERROR: $PY no es la versión $VERSION_ESPERADA (o no existe). Subir el .py nuevo con ese nombre exacto."
  exit 1
fi
# Para repetir solo una parte: SECCIONES=configuracion,datos bash diag-emisiones-v2.sh qa
SECCIONES="${SECCIONES:-codigo,orquestacion,configuracion,fuente,almacenamiento,datos,terceros,oracle}"
python3 "$DIR/diag_emisiones_v2.py" --amb "$AMB" --periodo "$PERIODO" --workgroup "$WG" --secciones "$SECCIONES"
