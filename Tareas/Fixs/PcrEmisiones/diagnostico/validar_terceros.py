#!/usr/bin/env python3
"""Valida acuerdos y tomadores (contra el RRC) y agentes (completitud) del Silver preparado.

Uso (local, sin AWS):
    python validar_terceros.py --rrc "RRC SIN CONTABILIDAD ALFA 202606.xlsx" \
        --evidencia <carpeta evidencia-pcr-emisiones-...> --salida informe-terceros.md

La evidencia (sección ``terceros`` de diag_emisiones_v2.py) trae, por producto, totales por
acuerdo y una huella SHA-256 de cada nombre de tomador o agente, nunca el nombre. Aquí se
calcula la misma huella con los nombres del RRC y se cruzan producto por producto.

- Acuerdos: el código de acuerdo no es dato personal; se cruza directo con la columna Acuerdo.
- Tomadores: la columna tomador del Silver de Generales puede traer varios nombres unidos con
  coma; desde la versión 2026-11-24.1 la evidencia trae cada nombre por separado.
- El RRC solo trae Generales, así que los cruces son solo de Generales.
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
import unicodedata
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
COLUMNAS_ACUERDO = ("numero_poliza_matriz", "nombre_acuerdo")
TOLERANCIA = Decimal("0.01")


def normalizar(nombre):
    return ESPACIOS.sub(" ", str(nombre or "")).strip(" ").upper()


def huella(nombre):
    return hashlib.sha256(normalizar(nombre).encode("utf-8")).hexdigest()


def dec(valor):
    return Decimal(str(valor)) if valor not in (None, "") else Decimal(0)


def leer_rrc(ruta, periodo):
    """Filas del RRC (hoja Data) del mes de producción, con huella del tomador."""
    hoja = openpyxl.load_workbook(ruta, read_only=True, data_only=True)["Data"]
    filas = hoja.iter_rows(values_only=True)
    encabezado = [str(h).strip() if h is not None else "" for h in next(filas)]
    ix = {h: i for i, h in enumerate(encabezado)}
    salida = []
    for fila in filas:
        if not fila or fila[ix["Producto"]] is None or str(fila[ix["Mes Produccion"]]) != periodo:
            continue
        salida.append({
            "producto": str(fila[ix["Producto"]]),
            "acuerdo": normalizar(fila[ix["Acuerdo"]]),
            "huella": huella(fila[ix["Tomador"]]),
            "nombre": normalizar(fila[ix["Tomador"]]),
            **{m: dec(fila[ix[m]]) for m in EQUIVALENCIAS},
        })
    return salida


def agrupar_rrc(filas, llave):
    grupos = defaultdict(lambda: {"filas": 0, "acuerdos": set(), "nombre": "", **{m: Decimal(0) for m in EQUIVALENCIAS}})
    for f in filas:
        g = grupos[llave(f)]
        g["filas"] += 1
        g["acuerdos"].add(f["acuerdo"])
        g["nombre"] = f["nombre"]
        for m in EQUIVALENCIAS:
            g[m] += f[m]
    return grupos


def huella_variante(nombre_normalizado, quitar_tildes):
    """Igual que el SQL: palabras en orden (codepoint) y, si se pide, sin marcas diacríticas (NFD)."""
    texto = nombre_normalizado
    if quitar_tildes:
        texto = "".join(c for c in unicodedata.normalize("NFD", texto) if not unicodedata.category(c).startswith("M"))
    return hashlib.sha256(" ".join(sorted(texto.split(" "))).encode("utf-8")).hexdigest()


def parejas_por_monto(filas_rrc, individual, producto, columna_acuerdo, exactas):
    """Tomadores del RRC sin pareja exacta con una sola pareja del Silver de igual prima y comisión
    en el mismo acuerdo. Devuelve [(palabras RRC, (palabras Silver, largo Silver))]."""
    if not columna_acuerdo:
        return []
    rrc = agrupar_rrc([f for f in filas_rrc if f["producto"] == producto], lambda f: (f["acuerdo"], f["huella"]))
    silver = defaultdict(lambda: {"prima": Decimal(0), "com": Decimal(0), "palabras": 0, "largo": 0})
    for f in individual:
        if producto_de(f) != producto or f["tomador_huella"].lower() in {h for _, h in rrc}:
            continue
        s = silver[(normalizar(f[columna_acuerdo]), f["tomador_huella"].lower())]
        s["prima"] += dec(f.get("prima")) + dec(f.get("prima_vida"))
        s["com"] += dec(f.get("comision_total_interincter"))
        s["palabras"], s["largo"] = int(dec(f.get("palabras_nombre"))), int(dec(f.get("largo_nombre")))
    usados, pares = set(), []
    for (acuerdo, h), r in rrc.items():
        if h in exactas:
            continue
        cand = [k for k, s in silver.items() if k[0] == acuerdo and k not in usados
                and abs(s["prima"] - r["Prima Emitida"]) <= TOLERANCIA and abs(s["com"] - r["Comision Intermediario"]) <= TOLERANCIA]
        if len(cand) == 1:
            usados.add(cand[0])
            pares.append((len(r["nombre"].split(" ")), (silver[cand[0]]["palabras"], silver[cand[0]]["largo"])))
    return pares


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


def agrupar_silver(filas, llave):
    grupos = defaultdict(lambda: defaultdict(Decimal))
    for f in filas:
        g = grupos[llave(f)]
        for k, v in f.items():
            if k in ("filas", "operaciones", "filas_varios_tomadores") or k in EQUIVALENCIAS or any(k in c for c in EQUIVALENCIAS.values()):
                g[k] += dec(v)
    return grupos


def silver_en(g, metrica):
    return sum((g.get(c, Decimal(0)) for c in EQUIVALENCIAS[metrica]), Decimal(0))


def comparar(rrc, silver, productos, etiqueta, detalle_rrc):
    """Tabla resumen y detalle del cruce RRC vs Silver con llaves (producto, x)."""
    resumen = [f"| Producto | {etiqueta} RRC | {etiqueta} Silver | En ambos | Solo RRC | Solo Silver | Montos distintos | Prima RRC en ambos | Prima Silver en ambos |",
               "|---|---|---|---|---|---|---|---|---|"]
    detalle, distintos_todos = [], []
    for p in productos:
        kr = {k for q, k in rrc if q == p}
        ks = {k for q, k in silver if q == p}
        comunes = kr & ks
        distintos = []
        for k in sorted(comunes):
            for metrica in EQUIVALENCIAS:
                a, b = rrc[(p, k)][metrica], silver_en(silver[(p, k)], metrica)
                if abs(a - b) > TOLERANCIA:
                    distintos.append((k, metrica, a, b))
        pr = sum((rrc[(p, k)]["Prima Emitida"] for k in comunes), Decimal(0))
        ps = sum((silver_en(silver[(p, k)], "Prima Emitida") for k in comunes), Decimal(0))
        resumen.append(f"| {p} | {len(kr)} | {len(ks)} | {len(comunes)} | {len(kr - ks)} | {len(ks - kr)} | "
                       f"{len({d[0] for d in distintos})} | {pr:,.2f} | {ps:,.2f} |")
        for k in sorted(kr - ks):
            detalle.append(f"| {p} | solo RRC | {detalle_rrc(p, k)} | {rrc[(p, k)]['Prima Emitida']:,.2f} | — |")
        for k in sorted(ks - kr, key=lambda k: -silver_en(silver[(p, k)], "Prima Emitida"))[:15]:
            detalle.append(f"| {p} | solo Silver | {detalle_rrc(p, k, solo_silver=True)} | — | {silver_en(silver[(p, k)], 'Prima Emitida'):,.2f} |")
        if len(ks - kr) > 15:
            detalle.append(f"| {p} | solo Silver | … y {len(ks - kr) - 15} más | — | — |")
        for k, metrica, a, b in distintos:
            detalle.append(f"| {p} | {metrica} distinta | {detalle_rrc(p, k)} | {a:,.2f} | {b:,.2f} |")
        distintos_todos += [(p, k, metrica) for k, metrica, _, _ in distintos]
    return resumen, detalle, distintos_todos


def main():
    parser = argparse.ArgumentParser(description="Valida acuerdos, tomadores y agentes del Silver preparado")
    parser.add_argument("--rrc", required=True)
    parser.add_argument("--evidencia", required=True)
    parser.add_argument("--periodo", default="202606")
    parser.add_argument("--salida", default="informe-terceros.md")
    parser.add_argument("--mostrar-nombres", action="store_true")
    args = parser.parse_args()

    datos = os.path.join(args.evidencia, "datos")
    lineas = [f"# Acuerdos, tomadores y agentes del Silver preparado ({os.path.basename(args.evidencia.rstrip('/'))})", ""]
    filas_rrc = leer_rrc(args.rrc, args.periodo)
    nombres_pantalla = []

    # ---- 1. Acuerdos
    lineas.append("## 1. Generales por acuerdo frente al RRC (Mes Produccion = periodo)")
    por_acuerdo = leer_csv(os.path.join(datos, "terceros_generales_por_acuerdo.csv"))
    columna_acuerdo = None
    if not por_acuerdo:
        lineas.append("La evidencia no trae `terceros_generales_por_acuerdo.csv` (requiere la versión 2026-11-24.1).")
    else:
        rrc_acu = agrupar_rrc(filas_rrc, lambda f: (f["producto"], f["acuerdo"]))
        # Se elige la columna del Silver cuyo valor coincide con más acuerdos del RRC.
        coincidencias = {}
        for col in COLUMNAS_ACUERDO:
            if col in por_acuerdo[0]:
                valores = {(producto_de(f), normalizar(f[col])) for f in por_acuerdo}
                coincidencias[col] = len(valores & set(rrc_acu))
        columna_acuerdo = max(coincidencias, key=coincidencias.get) if coincidencias else None
        lineas.append(f"Columna del Silver que coincide con `Acuerdo` del RRC: `{columna_acuerdo}` "
                      f"(coincidencias: {coincidencias}).")
        silver_acu = agrupar_silver(por_acuerdo, lambda f: (producto_de(f), normalizar(f[columna_acuerdo])))
        productos = sorted({p for p, _ in rrc_acu} | {p for p, _ in silver_acu})
        resumen, detalle, _ = comparar(rrc_acu, silver_acu, productos, "Acuerdos",
                                       lambda p, k, solo_silver=False: f"{k or '(vacío)'}")
        lineas += [""] + resumen
        if detalle:
            lineas += ["", "| Producto | Caso | Acuerdo | RRC (prima o métrica) | Silver |", "|---|---|---|---|---|"] + detalle
        else:
            lineas.append("\nTodos los acuerdos cuadran en prima, comisión del intermediario y comisión de recaudo.")

    # ---- 2. Forma de la columna tomador
    lineas += ["", "## 2. Forma de la columna `tomador` del Silver (varios nombres unidos con coma)"]
    for caso in ("generales", "satelites", "pcr_vida"):
        filas = leer_csv(os.path.join(datos, f"terceros_{caso}_tomador_repeticion.csv"))
        if filas:
            lineas += ["", f"**{caso}**", "", "| " + " | ".join(filas[0].keys()) + " |", "|" + "---|" * len(filas[0])]
            lineas += ["| " + " | ".join(str(v) for v in f.values()) + " |" for f in filas]

    # ---- 3. Tomadores individuales
    lineas += ["", "## 3. Tomadores de Generales frente al RRC (cada nombre por separado)"]
    individual = leer_csv(os.path.join(datos, "terceros_generales_por_tomador_individual.csv"))
    origen = "terceros_generales_por_tomador_individual.csv"
    if individual is None:
        individual = leer_csv(os.path.join(datos, "terceros_generales_por_tomador.csv"))
        origen = "terceros_generales_por_tomador.csv (lista completa sin separar: versión anterior)"
    if individual is None:
        lineas.append("La evidencia no trae tomadores de Generales (correr la sección `terceros`).")
    else:
        lineas.append(f"Fuente: `{origen}`. Si una fila trae varios tomadores distintos, sus montos cuentan en cada uno.")
        rrc_tom = agrupar_rrc(filas_rrc, lambda f: (f["producto"], f["huella"]))
        silver_tom = agrupar_silver(individual, lambda f: (producto_de(f), f["tomador_huella"].lower()))
        productos = sorted({p for p, _ in rrc_tom} | {p for p, _ in silver_tom})

        def ubicar(p, k, solo_silver=False):
            if solo_silver:
                return f"`{k[:10]}` | —"
            return f"`{k[:10]}` | {', '.join(sorted(rrc_tom[(p, k)]['acuerdos']))}"
        resumen, detalle, distintos = comparar(rrc_tom, silver_tom, productos, "Tomadores", ubicar)
        lineas += ["", "### 3a. Por producto y tomador", ""] + resumen
        if detalle:
            lineas += ["", "Huella = primeros 10 caracteres del SHA-256 del nombre normalizado.", "",
                       "| Producto | Caso | Huella | Acuerdo(s) RRC | RRC | Silver |", "|---|---|---|---|---|---|"] + detalle
        for p, k in sorted({(p, k) for p, k in rrc_tom} - set(silver_tom)):
            nombres_pantalla.append((p, "solo RRC", k[:10], rrc_tom[(p, k)]["nombre"]))
        for p, k, metrica in distintos:
            nombres_pantalla.append((p, f"{metrica} distinta", k[:10], rrc_tom[(p, k)]["nombre"]))

        if columna_acuerdo and columna_acuerdo in individual[0]:
            rrc_at = agrupar_rrc(filas_rrc, lambda f: (f["producto"], (f["acuerdo"], f["huella"])))
            silver_at = agrupar_silver(individual, lambda f: (producto_de(f), (normalizar(f[columna_acuerdo]), f["tomador_huella"].lower())))
            resumen, _, _ = comparar(rrc_at, silver_at, productos, "Acuerdo+tomador",
                                     lambda p, k, solo_silver=False: f"{k[0]} `{k[1][:10]}`")
            lineas += ["", "### 3b. Por producto, acuerdo y tomador", ""] + resumen

        if "huella_palabras_ordenadas" in individual[0]:
            lineas += ["", "### 3c. ¿El mismo tomador escrito distinto?", "",
                       "Para los tomadores del RRC sin pareja exacta: si cruzan con las palabras en otro orden, "
                       "o además sin tildes. Luego, emparejando por monto (misma prima y comisión en el mismo acuerdo, "
                       "pareja única), se compara el número de palabras y el largo del nombre.", "",
                       "| Producto | Sin pareja exacta | Cruzan con palabras en otro orden | Cruzan además sin tildes | Siguen sin cruzar | Pareja única por monto | Mismas palabras | Más palabras en el Silver | Menos palabras en el Silver |",
                       "|---|---|---|---|---|---|---|---|---|"]
            for p in productos:
                exactas = {h for q, h in silver_tom if q == p}
                orden = {f["huella_palabras_ordenadas"] for f in individual if producto_de(f) == p}
                orden_st = {f["huella_ordenada_sin_tildes"] for f in individual if producto_de(f) == p}
                pend = [k for q, k in rrc_tom if q == p and k not in exactas]
                por_orden = [k for k in pend if huella_variante(rrc_tom[(p, k)]["nombre"], False) in orden]
                por_tilde = [k for k in pend if k not in por_orden and huella_variante(rrc_tom[(p, k)]["nombre"], True) in orden_st]
                col_acu = columna_acuerdo or next((c for c in COLUMNAS_ACUERDO if c in individual[0]), None)
                pares = parejas_por_monto(filas_rrc, individual, p, col_acu, exactas)
                mismas = sum(1 for r, s in pares if r == s[0])
                mas = sum(1 for r, s in pares if s[0] > r)
                lineas.append(f"| {p} | {len(pend)} | {len(por_orden)} | {len(por_tilde)} | {len(pend) - len(por_orden) - len(por_tilde)} | "
                              f"{len(pares)} | {mismas} | {mas} | {len(pares) - mismas - mas} |")

    # ---- 4. Completitud
    lineas += ["", "## 4. Completitud de tomadores y agentes por caso"]
    for caso in ("generales", "satelites", "pcr_vida"):
        filas = leer_csv(os.path.join(datos, f"terceros_{caso}_completitud.csv"))
        if not filas:
            continue
        lineas += ["", f"**{caso}**", "", "| " + " | ".join(filas[0].keys()) + " |", "|" + "---|" * len(filas[0])]
        lineas += ["| " + " | ".join(str(v) for v in f.values()) + " |" for f in filas]

    # ---- 5. Agentes
    lineas += ["", "## 5. Agentes: reparto de la prima por producto (el RRC no trae agente)"]
    for caso in ("generales", "satelites", "pcr_vida"):
        filas = leer_csv(os.path.join(datos, f"terceros_{caso}_por_agente.csv"))
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
    print("\n".join(lineas[:60]))
    print(f"\nInforme: {args.salida}")
    if args.mostrar_nombres and nombres_pantalla:
        print("\n(Solo en pantalla) Tomadores del RRC que no cuadran:")
        for p, caso, h, nombre in nombres_pantalla:
            print(f"  {p} {caso} {h} {nombre}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
