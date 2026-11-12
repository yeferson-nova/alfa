#!/usr/bin/env bash
set -euo pipefail
AMBIENTE="${1:-}"
case "$AMBIENTE" in
  dev|qa|prd) ;;
  *) echo "Uso: bash scripts/empaquetar.sh <dev|qa|prd>" >&2; exit 2 ;;
esac
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NOMBRE="niif-${AMBIENTE}-notification-certification"
VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$RAIZ/src/notificacion_certificacion/__init__.py")"
if [ -z "$VERSION" ]; then
  echo "No se encontro __version__ en src/notificacion_certificacion/__init__.py" >&2
  exit 1
fi
DESTINO="$RAIZ/dist"
LIBRERIA="$DESTINO/${NOMBRE}-${VERSION}.zip"
SCRIPT="$DESTINO/job_notificacion.py"
mkdir -p "$DESTINO" && rm -f "$LIBRERIA" "$SCRIPT"
python3 - "$RAIZ/src" "$LIBRERIA" <<'PY'
import os
import sys
import zipfile

origen, destino = sys.argv[1:3]
with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as archivo:
    for raiz, carpetas, nombres in os.walk(os.path.join(origen, "notificacion_certificacion")):
        carpetas[:] = [carpeta for carpeta in carpetas if carpeta != "__pycache__"]
        for nombre in nombres:
            if nombre.endswith(".py"):
                ruta = os.path.join(raiz, nombre)
                archivo.write(ruta, os.path.relpath(ruta, origen))
PY
cp "$RAIZ/scripts/job_notificacion.py" "$SCRIPT"
echo "Libreria: $LIBRERIA  (--extra-py-files)"
echo "Script:   $SCRIPT  (ScriptLocation del job)"
