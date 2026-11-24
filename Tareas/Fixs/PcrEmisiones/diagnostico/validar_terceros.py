#!/usr/bin/env python3
"""Valida tomadores (contra el RRC) y agentes (completitud) del Silver preparado, por producto.

Uso (local, sin AWS):
    python validar_terceros.py --rrc "RRC SIN CONTABILIDAD ALFA 202606.xlsx" \
        --evidencia <carpeta evidencia-pcr-emisiones-...> --salida informe-terceros.md

La evidencia trae, por producto, una huella SHA-256 del nombre del tomador o agente
(sección ``terceros`` de diag_emisiones_v2.py), nunca el nombre. Aquí se calcula la misma
huella con los nombres del RRC y se cruzan producto por producto.

- Tomadores: el RRC solo trae Generales, así que el cruce es solo de Generales.
- Agentes: el RRC no tiene columna de agente; se informa completitud y reparto.
- El informe no escribe nombres. Con --mostrar-nombres se muestran en pantalla (no en el
  archivo) los tomadores del RRC que no cuadran, para buscarlos.
Requiere: openpyxl.
"""

import argparse
import csv
import hashlib
import os
import re
import sys
from collections import defaultdict
from decimal import Decimal

import openpyxl

# Mismo normalizado que el SQL de Athena: regexp_replace('\s+', ' ') (espacios ASCII de Java),
# trim y upper.
ESPACIOS = re.compile(r"[ \t\n\x0b\f\r]+")
EQUIVALENCIAS = {  # columna del Excel -> columnas del Silver de Generales
    "Prima Emitida": ("prima", "prima_vida"),
    "Comision Intermediario": ("comision_total_interincter",),
    "Comision Recaudo": ("costo_recaudacion", "costo_recaudacion_vida"),
}
TOLERANCIA = Decimal("0.01")


def normalizar(nombre):
    return ESPACIOS.sub(" ", str(nombre or "")).strip(" ").upper()


def huella(nombre):
    return hashlib.sha256(normalizar(nombre).encode("utf-8")).hexdigest()


def dec(valor):
    return Decimal(str(valor)) if valor not in (None, "") else Decimal(0)


def leer_rrc(ruta, periodo):
    hoja = openpyxl.load_workbook(ruta, read_only=True, data_only=True)["Data"]
    filas = hoja.iter_rows(values_only=True)
    encabezado = [str(h).strip() if h is not None else "" for h in next(filas)]
    ix = {h: i for i, h in enumerate(encabezado)}
    datos = defaultdict(lambda: {"filas": 0, "acuerdos": set(), "nombre": "", **{m: Decimal(0) for m in EQUIVALENCIAS}})
    for fila in filas:
        if not fila or fila[ix["Producto"]] is None or str(fila[ix["Mes Produccion"]]) != periodo:
            continue
        clave = (str(fila[ix["Producto"]]), huella(fila[ix["Tomador"]]))
        d = datos[clave]
        d["filas"] += 1
        d["acuerdos"].add(str(fila[ix["Acuerdo"]]))
        d["nombre"] = normalizar(fila[ix["Tomador"]])
        for metrica in EQUIVALENCIAS:
            d[metrica] += dec(fila[ix[metrica]])
    return datos


def leer_csv(ruta):
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding="utf-8") as archivo:
        return list(csv.DictReader(archivo))


def producto_de(fila):
    for columna in ("idproducto", "id_producto", "productid", "codigo_producto"):
        if fila.get(columna) not in (None, ""):
            return str(fila[columna]).split(".")[0]
    return "?"


def main():
    parser = argparse.ArgumentParser(description="Valida tomadores y agentes del Silver preparado")
    parser.add_argument("--rrc", required=True)
    parser.add_argument("--evidencia", required=True)
    parser.add_argument("--periodo", default="202606")
    parser.add_argument("--salida", default="informe-terceros.md")
    parser.add_argument("--mostrar-nombres", action="store_true")
    args = parser.parse_args()

    lineas = [f"# Tomadores y agentes del Silver preparado ({os.path.basename(args.evidencia.rstrip('/'))})", ""]
    rrc = leer_rrc(args.rrc, args.periodo)
    silver_csv = leer_csv(os.path.join(args.evidencia, "datos", "terceros_generales_por_tomador.csv"))
    nombres_pantalla = []

    lineas.append("## Tomadores de Generales frente al RRC (Mes Produccion = periodo)")
    if silver_csv is None:
        lineas.append("La evidencia no trae `terceros_generales_por_tomador.csv` (correr la sección `terceros`).")
    else:
        silver = defaultdict(lambda: defaultdict(Decimal))
        for fila in silver_csv:
            clave = (producto_de(fila), fila["tomador_huella"].lower())
            for k, v in fila.items():
                if k not in ("idproducto", "tomador_huella", "tomador_vacio"):
                    silver[clave][k] += dec(v)
            silver[clave]["vacio"] = Decimal(1) if str(fila.get("tomador_vacio")).lower() == "true" else Decimal(0)
        productos = sorted({p for p, _ in rrc} | {p for p, _ in silver})
        lineas.append("| Producto | Tomadores RRC | Tomadores Silver | En ambos | Solo RRC | Solo Silver | Montos distintos |")
        lineas.append("|---|---|---|---|---|---|---|")
        detalle = []
        for p in productos:
            hr = {h for q, h in rrc if q == p}
            hs = {h for q, h in silver if q == p}
            distintos = []
            for h in sorted(hr & hs):
                for metrica, cols in EQUIVALENCIAS.items():
                    a = rrc[(p, h)][metrica]
                    b = sum((silver[(p, h)].get(c, Decimal(0)) for c in cols), Decimal(0))
                    if abs(a - b) > TOLERANCIA:
                        distintos.append((h, metrica, a, b))
            lineas.append(f"| {p} | {len(hr)} | {len(hs)} | {len(hr & hs)} | {len(hr - hs)} | {len(hs - hr)} | {len({d[0] for d in distintos})} |")
            for h in sorted(hr - hs):
                r = rrc[(p, h)]
                detalle.append(f"| {p} | solo RRC | `{h[:10]}` | {', '.join(sorted(r['acuerdos']))} | {r['Prima Emitida']:,.2f} | — |")
                nombres_pantalla.append((p, "solo RRC", h[:10], r["nombre"]))
            for h in sorted(hs - hr):
                s = silver[(p, h)]
                prima = s.get("prima", Decimal(0)) + s.get("prima_vida", Decimal(0))
                vacio = " (tomador vacío)" if s.get("vacio") else ""
                detalle.append(f"| {p} | solo Silver{vacio} | `{h[:10]}` | — | — | {prima:,.2f} |")
            for h, metrica, a, b in distintos:
                detalle.append(f"| {p} | {metrica} distinta | `{h[:10]}` | {', '.join(sorted(rrc[(p, h)]['acuerdos']))} | {a:,.2f} | {b:,.2f} |")
                nombres_pantalla.append((p, f"{metrica} distinta", h[:10], rrc[(p, h)]["nombre"]))
        if detalle:
            lineas += ["", "Detalle (huella = primeros 10 caracteres del SHA-256 del nombre normalizado; acuerdo según el RRC):", "",
                       "| Producto | Caso | Huella | Acuerdo(s) RRC | RRC | Silver |", "|---|---|---|---|---|---|"] + detalle
        else:
            lineas.append("\nTodos los tomadores cuadran en prima, comisión del intermediario y comisión de recaudo.")

    lineas += ["", "## Completitud de tomadores y agentes por caso"]
    for caso in ("generales", "satelites", "pcr_vida"):
        filas = leer_csv(os.path.join(args.evidencia, "datos", f"terceros_{caso}_completitud.csv"))
        if not filas:
            continue
        lineas += ["", f"**{caso}**", "", "| " + " | ".join(filas[0].keys()) + " |", "|" + "---|" * len(filas[0])]
        lineas += ["| " + " | ".join(str(v) for v in f.values()) + " |" for f in filas]

    lineas += ["", "## Agentes: reparto de la prima por producto (el RRC no trae agente)"]
    for caso in ("generales", "satelites", "pcr_vida"):
        filas = leer_csv(os.path.join(args.evidencia, "datos", f"terceros_{caso}_por_agente.csv"))
        if not filas:
            continue
        por_producto = defaultdict(list)
        for f in filas:
            por_producto[producto_de(f)].append(f)
        lineas += ["", f"**{caso}**", "", "| Producto | Agentes | Filas sin agente | Prima sin agente | Prima total |", "|---|---|---|---|---|"]
        for p, grupo in sorted(por_producto.items()):
            vacios = [g for g in grupo if str(g.get("agente_vacio")).lower() == "true"]
            prima = lambda gs: sum((dec(g.get("prima")) + dec(g.get("prima_vida")) for g in gs), Decimal(0))
            lineas.append(f"| {p} | {len(grupo) - len(vacios)} | {sum(int(dec(g['filas'])) for g in vacios)} | {prima(vacios):,.2f} | {prima(grupo):,.2f} |")

    with open(args.salida, "w", encoding="utf-8") as archivo:
        archivo.write("\n".join(lineas) + "\n")
    print("\n".join(lineas[:40]))
    print(f"\nInforme: {args.salida}")
    if args.mostrar_nombres and nombres_pantalla:
        print("\n(Solo en pantalla) Tomadores del RRC que no cuadran:")
        for p, caso, h, nombre in nombres_pantalla:
            print(f"  {p} {caso} {h} {nombre}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
