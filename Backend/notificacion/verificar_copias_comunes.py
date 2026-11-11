import hashlib
import sys
from pathlib import Path

PAQUETE_COMUN = "niif_observability"
RAIZ = Path(__file__).resolve().parent


def huella(carpeta: Path) -> str:
    resumen = hashlib.sha256()
    for archivo in sorted(carpeta.rglob("*.py")):
        resumen.update(archivo.relative_to(carpeta).as_posix().encode("utf-8"))
        resumen.update(archivo.read_bytes().replace(b"\r\n", b"\n"))
    return resumen.hexdigest()


def main() -> int:
    copias = {
        f"{carpeta.parents[2].name}/{carpeta.parents[1].name}": huella(carpeta)
        for carpeta in sorted(RAIZ.glob(f"*/*/src/{PAQUETE_COMUN}"))
    }
    for componente, valor in copias.items():
        print(f"{valor[:12]}  {componente}")
    if len(set(copias.values())) > 1:
        print(f"DIFIEREN: las copias de {PAQUETE_COMUN} no son iguales. Copiar la version correcta a todos.")
        return 1
    print(f"OK: {len(copias)} copia(s) de {PAQUETE_COMUN} iguales.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
