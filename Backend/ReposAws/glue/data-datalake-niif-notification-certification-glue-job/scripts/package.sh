#!/usr/bin/env bash
set -euo pipefail
ENVIRONMENT="${1:-}"
case "$ENVIRONMENT" in
  dev|qa|prd) ;;
  *) echo "Uso: bash scripts/package.sh <dev|qa|prd>" >&2; exit 2 ;;
esac
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE="certification_notifier"
NAME="niif-${ENVIRONMENT}-notification-certification"
VERSION="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "$ROOT/src/$PACKAGE/__init__.py")"
if [ -z "$VERSION" ]; then
  echo "No se encontro __version__ en src/$PACKAGE/__init__.py" >&2
  exit 1
fi
OUTPUT="$ROOT/dist"
LIBRARY="$OUTPUT/${NAME}-${VERSION}.zip"
SCRIPT="$OUTPUT/notification_certification.py"
mkdir -p "$OUTPUT" && rm -f "$LIBRARY" "$SCRIPT"
python3 - "$ROOT/src" "$LIBRARY" "$PACKAGE" <<'PY'
import os
import sys
import zipfile

source, target, package = sys.argv[1:4]
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
    for root, folders, names in os.walk(os.path.join(source, package)):
        folders[:] = [folder for folder in folders if folder != "__pycache__"]
        for name in names:
            if name.endswith(".py"):
                path = os.path.join(root, name)
                archive.write(path, os.path.relpath(path, source))
PY
cp "$ROOT/scripts/notification_certification.py" "$SCRIPT"
echo "Libreria: $LIBRARY  (--extra-py-files)"
echo "Script:   $SCRIPT  (ScriptLocation del job)"
