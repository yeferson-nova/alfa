#!/usr/bin/env bash
set -euo pipefail
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NOMBRE="niif-dev-notification-certification"
VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$RAIZ/src/notificacion_certificacion/__init__.py")"
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
