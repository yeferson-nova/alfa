"""Consolida una salida de medir-tiempos-dev.sh en el registro de tiempos de la malla.

Uso (en el equipo, no en AWS):
    python consolidar-tiempos.py <salida.txt> --version <etiqueta> --sesiones <texto> [--registro <csv>]

Cada corrida del script agrega (o reemplaza, si ya existe) las filas de esa medicion en
../../tiempos/registro-tiempos.csv. Ese CSV es la fuente del grafico de tiempos de la entrega.

Niveles:
    job    tiempo de reloj y DPU-hora de cada corrida exitosa del job (seccion 1)
    caso   suma del tiempo de las tablas de cada caso por lote (seccion 5)
    tabla  tiempo de lectura y escritura, y filas de cada tabla por lote (seccion 5)
"""
import argparse
import csv
import os
import re
from datetime import datetime, timedelta

COLUMNAS = ["id_medicion", "version_codigo", "sesiones_oracle", "lote", "inicio_corrida", "nivel",
            "caso", "tabla", "medida", "minutos", "filas", "dpu_h", "estado"]
RE_JOB = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+SUCCEEDED\s+(\d+)\s+([\d.]+)\s+\S+")
RE_TABLA = re.compile(r"^(\S+)\s+(\d{14})\s+(-?\d+)\s+(-?\d+)\s+(\S+)\s+(\S+)\s+(\S+)$")
RE_CASO = re.compile(r"^-- (\S+): lote=(\d{14}) .*?fin=(\S+) tablas=")


def lote_de(inicio, lotes):
    """El lote es la hora UTC en que la configuradora arranca la corrida (AAAAMMDDhhmmss)."""
    for lote in lotes:
        t = datetime.strptime(lote, "%Y%m%d%H%M%S")
        if abs(t - inicio) <= timedelta(minutes=2):
            return lote
    return ""


def leer(salida):
    jobs, tablas, fines = [], [], []
    seccion = 0
    for linea in open(salida, encoding="utf-8"):
        linea = linea.rstrip("\n")
        m = re.match(r"^== (\d)\.", linea)
        if m:
            seccion = int(m.group(1))
            continue
        if seccion == 1:
            j = RE_JOB.match(linea)
            if j:
                jobs.append((datetime.strptime(j.group(1), "%Y-%m-%d %H:%M:%S"), int(j.group(2)), float(j.group(3))))
        elif seccion == 4:
            c = RE_CASO.match(linea)
            if c:
                fin = datetime.fromisoformat(c.group(3)).replace(tzinfo=None)
                fines.append((c.group(1).replace("-", "_"), c.group(2), fin))
        elif seccion == 5:
            t = RE_TABLA.match(linea.strip())
            if t:
                caso, lote, escr, conteo, filas, estado, tabla = t.groups()
                tablas.append({"caso": caso, "lote": lote, "seg": max(int(escr), 0) + max(int(conteo), 0),
                               "filas": filas, "estado": estado, "tabla": tabla})
    return jobs, tablas, fines


def filas_registro(id_medicion, version, sesiones, jobs, tablas, fines):
    lotes = sorted({t["lote"] for t in tablas} | {f[1] for f in fines})
    base = {"id_medicion": id_medicion, "version_codigo": version, "sesiones_oracle": sesiones}
    filas = []
    # Reloj por caso: los casos corren en serie; cada uno va desde el fin del anterior
    # (el primero, desde el inicio del job, e incluye el nucleo comun).
    inicio_lote = {lote_de(i, lotes): i for i, _, _ in jobs}
    anterior = {}
    for caso, lote, fin in sorted(fines, key=lambda x: x[2]):
        desde = anterior.get(lote) or inicio_lote.get(lote)
        if desde:
            filas.append({**base, "lote": lote, "inicio_corrida": "", "nivel": "caso", "caso": caso, "tabla": "",
                          "medida": "reloj" + ("_incluye_nucleo" if lote not in anterior else ""),
                          "minutos": round((fin - desde).total_seconds() / 60, 1), "filas": "", "dpu_h": "", "estado": ""})
        anterior[lote] = fin
    for inicio, minutos, dpu in jobs:
        lote = lote_de(inicio, lotes)
        filas.append({**base, "lote": lote, "inicio_corrida": inicio.isoformat(sep=" "), "nivel": "job",
                      "caso": "", "tabla": "", "medida": "reloj", "minutos": minutos, "filas": "",
                      "dpu_h": dpu, "estado": "SUCCEEDED"})
    suma = {}
    for t in tablas:
        suma[(t["lote"], t["caso"])] = suma.get((t["lote"], t["caso"]), 0) + t["seg"]
        filas.append({**base, "lote": t["lote"], "inicio_corrida": "", "nivel": "tabla", "caso": t["caso"],
                      "tabla": t["tabla"], "medida": "lectura_escritura_conteo", "minutos": round(t["seg"] / 60, 2),
                      "filas": t["filas"], "dpu_h": "", "estado": t["estado"]})
    for (lote, caso), seg in sorted(suma.items()):
        filas.append({**base, "lote": lote, "inicio_corrida": "", "nivel": "caso", "caso": caso, "tabla": "",
                      "medida": "suma_tablas", "minutos": round(seg / 60, 1), "filas": "", "dpu_h": "", "estado": ""})
    return filas


def main():
    aqui = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser()
    p.add_argument("salida")
    p.add_argument("--version", required=True, help="ej. QA-2086a5a0 (version del script del job medida)")
    p.add_argument("--sesiones", required=True, help="sesiones Oracle de esa version, ej. 'hasta 3 + 4 particiones'")
    p.add_argument("--registro", default=os.path.join(aqui, "..", "..", "tiempos", "registro-tiempos.csv"))
    a = p.parse_args()

    id_medicion = os.path.splitext(os.path.basename(a.salida))[0]
    jobs, tablas, fines = leer(a.salida)
    nuevas = filas_registro(id_medicion, a.version, a.sesiones, jobs, tablas, fines)

    existentes = []
    if os.path.exists(a.registro):
        with open(a.registro, encoding="utf-8-sig", newline="") as f:
            existentes = [r for r in csv.DictReader(f) if r["id_medicion"] != id_medicion]
    os.makedirs(os.path.dirname(a.registro), exist_ok=True)
    with open(a.registro, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNAS)
        w.writeheader()
        w.writerows(existentes + nuevas)
    print(f"{id_medicion}: job={len(jobs)} tablas={len(tablas)} filas_registro={len(nuevas)} -> {os.path.normpath(a.registro)}")


if __name__ == "__main__":
    main()
