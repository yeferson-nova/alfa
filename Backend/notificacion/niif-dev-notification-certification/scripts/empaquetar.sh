#!/usr/bin/env bash
set -euo pipefail
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NOMBRE="niif-dev-notification-certification"
VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$RAIZ/src/notificacion_certificacion/__init__.py")"
DESTINO="$RAIZ/dist"
LIBRERIA="$DESTINO/${NOMBRE}-${VERSION}.zip"
SCRIPT="$DESTINO/job_notificacion.py"
PY=""
for candidato in python3 python py; do
  if "$candidato" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" >/dev/null 2>&1; then
    PY="$candidato"; break
  fi
done
[ -n "$PY" ] || { echo "No se encontro Python 3.10 o superior (python3, python o py)" >&2; exit 1; }
mkdir -p "$DESTINO" && rm -f "$LIBRERIA" "$SCRIPT"
"$PY" - "$LIBRERIA" "$RAIZ/src" notificacion_certificacion "$RAIZ/src" niif_observability <<'PY'
import os
import sys
import zipfile

destino, *pares = sys.argv[1:]
with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as archivo:
    for origen, paquete in zip(pares[::2], pares[1::2]):
        carpeta_paquete = os.path.join(origen, paquete)
        if not os.path.isdir(carpeta_paquete):
            sys.exit(f"No existe el paquete {carpeta_paquete}")
        for raiz, carpetas, nombres in os.walk(carpeta_paquete):
            carpetas[:] = [carpeta for carpeta in carpetas if carpeta != "__pycache__"]
            for nombre in nombres:
                if nombre.endswith(".py"):
                    ruta = os.path.join(raiz, nombre)
                    archivo.write(ruta, os.path.relpath(ruta, origen))
PY
cp "$RAIZ/scripts/job_notificacion.py" "$SCRIPT"
echo "Libreria: $LIBRERIA  (--extra-py-files)"
echo "Script:   $SCRIPT  (ScriptLocation del job)"
