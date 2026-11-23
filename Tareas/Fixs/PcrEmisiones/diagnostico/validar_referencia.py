#!/usr/bin/env python3
"""Convierte los Excel de referencia de negocio en totales esperados por producto.

Uso (local, no necesita AWS):
    python validar_referencia.py --rrc "RRC SIN CONTABILIDAD ALFA 202606.xlsx" \
        --resumen "Resumen emision 202606.xlsx" --salida referencia_esperada.json

Qué hace:
1. Recalcula los totales de Generales desde la hoja ``Data`` del RRC (detalle).
2. Comprueba que el ``Resumen`` sale de ese detalle (PASS/FAIL).
3. Lista las diferencias que el propio archivo reporta contra otro informe (E1-A1)
   y las de Vida contra Satélites.
4. Escribe ``referencia_esperada.json`` con dos variantes para Generales:
   ``mes_produccion_periodo`` (lo que usa el resumen) y ``todo_el_corte``
   (todas las filas del corte, sin filtrar el mes de producción).

La salida solo tiene agregados por producto: no copia nombres ni documentos.
Requiere: openpyxl.
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP

import openpyxl

METRICAS = [
    "Prima Emitida",
    "Prima Alfa",
    "Comision Intermediario",
    "Iva Comision Intermediario",
    "Comision Recaudo",
    "Iva Comision Recaudo",
]
COLUMNAS_RESUMEN = dict(zip(METRICAS, "DEFGHI"))
CENTAVO = Decimal("0.01")


def a_decimal(valor):
    if valor in (None, ""):
        return Decimal(0)
    return Decimal(str(valor))


def redondear(valor):
    return valor.quantize(CENTAVO, ROUND_HALF_UP)


def leer_detalle_rrc(ruta, periodo):
    hoja = openpyxl.load_workbook(ruta, read_only=True, data_only=True)["Data"]
    filas = hoja.iter_rows(values_only=True)
    encabezado = [str(h).strip() if h is not None else "" for h in next(filas)]
    indice = {nombre: i for i, nombre in enumerate(encabezado)}
    faltantes = [m for m in METRICAS + ["Mes Produccion", "Producto"] if m not in indice]
    if faltantes:
        raise ValueError(f"La hoja Data no tiene las columnas {faltantes}")

    del_periodo = defaultdict(lambda: defaultdict(Decimal))
    todo = defaultdict(lambda: defaultdict(Decimal))
    filas_periodo = Counter()
    filas_todo = Counter()
    meses = Counter()
    cortes = Counter()
    for fila in filas:
        if fila is None or all(v is None for v in fila):
            continue
        mes = str(fila[indice["Mes Produccion"]])
        producto = str(fila[indice["Producto"]])
        meses[mes] += 1
        cortes[(str(fila[indice["Fecha Desde Corte"]]), str(fila[indice["Fecha Hasta Corte"]]))] += 1
        filas_todo[producto] += 1
        for metrica in METRICAS:
            valor = a_decimal(fila[indice[metrica]])
            todo[producto][metrica] += valor
            if mes == periodo:
                del_periodo[producto][metrica] += valor
        if mes == periodo:
            filas_periodo[producto] += 1
    return {
        "del_periodo": del_periodo,
        "todo": todo,
        "filas_periodo": filas_periodo,
        "filas_todo": filas_todo,
        "meses": meses,
        "cortes": cortes,
    }


def bloque_resumen(hoja, filas, columna_producto):
    salida = {}
    for fila in filas:
        producto = hoja[f"{columna_producto}{fila}"].value
        if producto is None:
            continue
        salida[str(producto)] = {
            metrica: str(redondear(a_decimal(hoja[f"{col}{fila}"].value)))
            for metrica, col in COLUMNAS_RESUMEN.items()
        }
    return salida


def como_texto(totales):
    return {
        producto: {m: str(redondear(v)) for m, v in metricas.items()}
        for producto, metricas in sorted(totales.items())
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rrc", required=True)
    parser.add_argument("--resumen", required=True)
    parser.add_argument("--periodo", default="202606")
    parser.add_argument("--salida", default="referencia_esperada.json")
    args = parser.parse_args()

    detalle = leer_detalle_rrc(args.rrc, args.periodo)
    print(f"Filas de detalle RRC: {sum(detalle['filas_todo'].values())}")
    print(f"Mes de producción (filas): {dict(sorted(detalle['meses'].items()))}")
    print(f"Cortes (desde, hasta): {dict(detalle['cortes'])}")

    hoja = openpyxl.load_workbook(args.resumen, data_only=True)["Hoja1"]
    generales = bloque_resumen(hoja, range(5, 10), "B")
    pymes = bloque_resumen(hoja, range(17, 18), "B")
    vida = bloque_resumen(hoja, range(24, 32), "B")
    satelites = bloque_resumen(hoja, range(39, 46), "A")

    print(f"\n[1] Resumen de Generales frente al detalle RRC con Mes Produccion = {args.periodo}")
    fallas = 0
    for producto, metricas in generales.items():
        for metrica, valor in metricas.items():
            calculado = redondear(detalle["del_periodo"].get(producto, {}).get(metrica, Decimal(0)))
            if Decimal(valor) != calculado:
                fallas += 1
                print(f"   DIF {producto} {metrica}: resumen={valor} detalle={calculado}")
    print("   PASS" if not fallas else f"   FAIL ({fallas} diferencias)")

    print("\n[2] Diferencias que reporta el propio archivo (fila E1-A1 frente al RRC)")
    diferencias_archivo = {}
    for fila, bloque in ((13, "generales"), (35, "vida")):
        fila_titulos = 4 if fila < 20 else 23
        diferencias_archivo[bloque] = {
            hoja[f"{col}{fila_titulos}"].value: str(redondear(a_decimal(hoja[f"{col}{fila}"].value)))
            for col in "DEFGHI"
            if hoja[f"{col}{fila}"].value is not None
        }
        print(f"   {bloque}: {diferencias_archivo[bloque]}")

    print("\n[3] Vida (RRC) frente a Satélites, mismo producto")
    for producto, metricas in vida.items():
        if producto not in satelites:
            continue
        for metrica, valor in metricas.items():
            otro = satelites[producto][metrica]
            if valor != otro:
                print(f"   {producto} {metrica}: vida={valor} satelites={otro} dif={Decimal(valor) - Decimal(otro)}")

    referencia = {
        "periodo": args.periodo,
        "origen": "Excel de negocio (RRC sin contabilidad y Resumen emision)",
        "nota": "Solo agregados por producto. Los nombres de columnas del Excel no equivalen 1 a 1 a las del Silver: ver mapeo-columnas-propuesta.json.",
        "generales": {
            "mes_produccion_periodo": como_texto(detalle["del_periodo"]),
            "todo_el_corte": como_texto(detalle["todo"]),
            "filas_mes_produccion_periodo": dict(detalle["filas_periodo"]),
            "filas_todo_el_corte": dict(detalle["filas_todo"]),
            "resumen": generales,
        },
        "pymes": pymes,
        "vida": vida,
        "satelites": satelites,
        "diferencias_reportadas_en_el_archivo": diferencias_archivo,
    }
    with open(args.salida, "w", encoding="utf-8") as archivo:
        json.dump(referencia, archivo, ensure_ascii=False, indent=2)
    print(f"\nEscrito {args.salida}")
    return 1 if fallas else 0


if __name__ == "__main__":
    sys.exit(main())
