#!/usr/bin/env bash
set -euo pipefail
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NOMBRE="niif-dev-notification-certification-orchestrator"
VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$RAIZ/src/certification_orchestrator/__init__.py")"
DESTINO="$RAIZ/dist"
PAQUETE="$DESTINO/${NOMBRE}-${VERSION}.zip"
PY=""
for candidato in python3 python py; do
  if "$candidato" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" >/dev/null 2>&1; then
    PY="$candidato"; break
  fi
done
[ -n "$PY" ] || { echo "No se encontro Python 3.10 o superior (python3, python o py)" >&2; exit 1; }
mkdir -p "$DESTINO" && rm -f "$PAQUETE"
"$PY" - "$PAQUETE" "$RAIZ/src" lambda_function.py certification_orchestrator niif_observability <<'PY'
import os
import sys
import zipfile

destino, origen, *entradas = sys.argv[1:]
with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as archivo:
    for entrada in entradas:
        ruta = os.path.join(origen, entrada)
        if os.path.isfile(ruta):
            archivo.write(ruta, entrada)
            continue
        if not os.path.isdir(ruta):
            sys.exit(f"No existe {ruta}")
        for raiz, carpetas, nombres in os.walk(ruta):
            carpetas[:] = [carpeta for carpeta in carpetas if carpeta != "__pycache__"]
            for nombre in nombres:
                if nombre.endswith(".py"):
                    completo = os.path.join(raiz, nombre)
                    archivo.write(completo, os.path.relpath(completo, origen).replace(os.sep, "/"))
PY
echo "Paquete de la lambda: $PAQUETE  (handler: lambda_function.lambda_handler)"
