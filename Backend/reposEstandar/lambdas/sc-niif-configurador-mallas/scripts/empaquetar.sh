#!/usr/bin/env bash
# Empaqueta el código de la lambda (sin dependencias: boto3 viene en el runtime de Lambda).
# Uso: bash scripts/empaquetar.sh   ->   dist/sc-niif-configurador-mallas-<version>.zip
set -euo pipefail
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$RAIZ/src/configurador_mallas/__init__.py")"
DESTINO="$RAIZ/dist/sc-niif-configurador-mallas-${VERSION}.zip"
mkdir -p "$RAIZ/dist" && rm -f "$DESTINO"
python3 - "$RAIZ/src" "$DESTINO" <<'PY'
import os, sys, zipfile
src, destino = sys.argv[1:3]
with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
    for raiz, dirs, archivos in os.walk(os.path.join(src, "configurador_mallas")):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for a in archivos:
            if a.endswith(".py"):
                ruta = os.path.join(raiz, a)
                z.write(ruta, os.path.relpath(ruta, src))
PY
echo "Artefacto: $DESTINO"
echo "Handler:   configurador_mallas.adaptadores.entrada.lambda_handler.lambda_handler"
