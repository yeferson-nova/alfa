#!/usr/bin/env python3
"""Compara dos evidencias del diagnóstico (p. ej. QA y PRD) capa por capa y contra el Excel.

Uso (local, sin AWS):
    python comparar_ambientes.py --a evidencia-pcr-emisiones-qa-... --b evidencia-pcr-emisiones-prd-... \
        --referencia referencia_esperada.json --mapeo mapeo-columnas-propuesta.json --salida informe.md

Cada evidencia es la carpeta (descomprimida) que deja ``diagnostico_pcr_emisiones.py``.
Si el DBA corrió ``conciliacion-oracle-acsele.sql``, poner sus CSV en ``<evidencia>/oracle_resultados/``
con el mismo nombre en los dos ambientes (p. ej. ``Q5.csv``) y se comparan también.

Orden de las capas (la primera que difiere es donde nace la diferencia):
  1. Fuente Oracle      2. Código desplegado      3. Orquestación
  4. Configuración      5. Extracción (runtime)   6. Bronce      7. Silver
Y aparte: Silver de cada ambiente frente al Excel (con el mapeo propuesto).
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from decimal import Decimal, InvalidOperation

PATRON_AMBIENTE = re.compile(r"\b((?:niif|alfa-datalake|datalake)-)(dev|qa|prd|prod)(-)", re.I)
PATRON_VALOR_AMBIENTE = re.compile(r'"(dev|qa|prd|prod)"', re.I)
PATRON_CUENTA = re.compile(r"\b\d{12}\b")
CENTAVO = Decimal("0.01")
CASOS = ("generales", "satelites", "pcr_vida")


def normalizar(texto):
    """Quita lo que siempre cambia entre ambientes (nombre del ambiente y número de cuenta)."""
    texto = PATRON_AMBIENTE.sub(r"\1{env}\3", texto or "")
    texto = PATRON_VALOR_AMBIENTE.sub('"{env}"', texto)
    return PATRON_CUENTA.sub("{cuenta}", texto)


def leer_json(ruta, defecto=None):
    try:
        with open(ruta, encoding="utf-8") as archivo:
            return json.load(archivo)
    except FileNotFoundError:
        return defecto


def leer_texto(ruta):
    try:
        with open(ruta, encoding="utf-8", errors="replace") as archivo:
            return archivo.read()
    except FileNotFoundError:
        return None


def leer_csv(ruta):
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding="utf-8-sig", newline="") as archivo:
        return [{k.lower(): v for k, v in fila.items()} for fila in csv.DictReader(archivo)]


def dec(valor):
    try:
        return Decimal(str(valor).replace(",", "")) if valor not in (None, "") else Decimal(0)
    except InvalidOperation:
        return Decimal(0)


class Informe:
    def __init__(self):
        self.lineas = []
        self.capas = {}

    def titulo(self, texto):
        self.lineas.append(f"\n## {texto}\n")

    def __call__(self, texto=""):
        self.lineas.append(texto)

    def capa(self, nombre, estado, detalle=""):
        self.capas[nombre] = (estado, detalle)

    def texto(self):
        return "\n".join(self.lineas)


# ------------------------------------------------------------------ capas

def comparar_oracle(inf, a, b, na, nb):
    inf.titulo("1. Fuente Oracle (resultados del DBA)")
    da, db = os.path.join(a, "oracle_resultados"), os.path.join(b, "oracle_resultados")
    if not (os.path.isdir(da) and os.path.isdir(db)):
        inf("Sin resultados de Oracle en las dos evidencias: la fuente **no está comparada** todavía.")
        inf.capa("1. Fuente Oracle", "SIN DATOS")
        return
    distintos = []
    for nombre in sorted(set(os.listdir(da)) & set(os.listdir(db))):
        filas_a, filas_b = leer_csv(os.path.join(da, nombre)), leer_csv(os.path.join(db, nombre))
        conjunto_a = {json.dumps(f, sort_keys=True) for f in filas_a or []}
        conjunto_b = {json.dumps(f, sort_keys=True) for f in filas_b or []}
        if conjunto_a == conjunto_b:
            inf(f"- {nombre}: IGUAL ({len(conjunto_a)} filas)")
        else:
            distintos.append(nombre)
            inf(f"- {nombre}: **DISTINTO** (solo en {na}: {len(conjunto_a - conjunto_b)}, solo en {nb}: {len(conjunto_b - conjunto_a)})")
            for fila in sorted(conjunto_a - conjunto_b)[:15]:
                inf(f"    - {na}: {fila}")
            for fila in sorted(conjunto_b - conjunto_a)[:15]:
                inf(f"    - {nb}: {fila}")
    inf.capa("1. Fuente Oracle", "DIFIERE" if distintos else "IGUAL", ", ".join(distintos))


def comparar_codigo(inf, a, b, na, nb):
    inf.titulo("2. Código desplegado")
    ra = leer_json(os.path.join(a, "codigo", "resumen_codigo.json"), {})
    rb = leer_json(os.path.join(b, "codigo", "resumen_codigo.json"), {})
    if not ra or not rb:
        inf("Falta la sección de código en alguna evidencia.")
        inf.capa("2. Código", "SIN DATOS")
        return
    distintos = []
    jobs = sorted({normalizar(j) for j in list(ra.get("glue", {})) + list(rb.get("glue", {}))})
    inf(f"| Job | {na} | {nb} | Resultado |\n|---|---|---|---|")
    for job in jobs:
        ja = next((v | {"_n": k} for k, v in ra.get("glue", {}).items() if normalizar(k) == job), {})
        jb = next((v | {"_n": k} for k, v in rb.get("glue", {}).items() if normalizar(k) == job), {})
        ta = _script(a, ja)
        tb = _script(b, jb)
        if ta is None or tb is None:
            resultado = "sin script"
        elif ta == tb:
            resultado = "IGUAL"
        elif normalizar(ta) == normalizar(tb):
            resultado = "igual salvo nombres de ambiente"
        else:
            resultado = "**DISTINTO**"
            distintos.append(job)
        inf(f"| {job} | {str(ja.get('sha256_script', '-'))[:12]} | {str(jb.get('sha256_script', '-'))[:12]} | {resultado} |")
        args_a = {k: normalizar(str(v)) for k, v in (ja.get("argumentos_por_defecto") or {}).items()}
        args_b = {k: normalizar(str(v)) for k, v in (jb.get("argumentos_por_defecto") or {}).items()}
        for clave in sorted(set(args_a) | set(args_b)):
            if args_a.get(clave) != args_b.get(clave):
                inf(f"|  ↳ argumento {clave} | {args_a.get(clave)} | {args_b.get(clave)} | distinto |")
    for funcion_a, la in (ra.get("lambda") or {}).items():
        lb = next((v for k, v in (rb.get("lambda") or {}).items() if normalizar(k) == normalizar(funcion_a)), {})
        archivos = sorted(set(la.get("archivos", {})) | set(lb.get("archivos", {})))
        for nombre in archivos:
            ta = leer_texto(os.path.join(a, "codigo", "lambda", funcion_a, nombre))
            tb = leer_texto(os.path.join(b, "codigo", "lambda", next(iter(rb.get("lambda", {})), ""), nombre))
            if ta == tb:
                resultado = "IGUAL"
            elif ta is not None and tb is not None and normalizar(ta) == normalizar(tb):
                resultado = "igual salvo nombres de ambiente"
            else:
                resultado = "**DISTINTO**"
                distintos.append(f"lambda/{nombre}")
            inf(f"| lambda {normalizar(funcion_a)}/{nombre} | {str(la.get('archivos', {}).get(nombre, '-'))[:12]} | {str(lb.get('archivos', {}).get(nombre, '-'))[:12]} | {resultado} |")
    inf.capa("2. Código", "DIFIERE" if distintos else "IGUAL", ", ".join(distintos))


def _script(raiz, job):
    if not job.get("script"):
        return None
    return leer_texto(os.path.join(raiz, "codigo", "glue", job["_n"], os.path.basename(job["script"])))


def comparar_orquestacion(inf, a, b, na, nb):
    inf.titulo("3. Orquestación (Step Function)")
    da = leer_texto(os.path.join(a, "orquestacion", "definicion_step_function.json"))
    db = leer_texto(os.path.join(b, "orquestacion", "definicion_step_function.json"))
    if da is None or db is None:
        inf("Falta la definición en alguna evidencia.")
        inf.capa("3. Orquestación", "SIN DATOS")
        return
    ja, jb = json.loads(normalizar(da)), json.loads(normalizar(db))
    igual = ja == jb
    inf("Definición: " + ("IGUAL (normalizando el ambiente y la cuenta)" if igual else "**DISTINTA**"))
    if not igual:
        for nombre in sorted(set(_estados(ja)) | set(_estados(jb))):
            ea, eb = _estados(ja).get(nombre), _estados(jb).get(nombre)
            if ea != eb:
                inf(f"- Estado `{nombre}`: {na}={json.dumps(ea, ensure_ascii=False)[:300]} | {nb}={json.dumps(eb, ensure_ascii=False)[:300]}")
    for etiqueta, raiz in ((na, a), (nb, b)):
        ejecuciones = leer_json(os.path.join(raiz, "orquestacion", "ejecuciones.json"), []) or []
        inf(f"\nÚltimas ejecuciones en {etiqueta}:")
        for e in ejecuciones[:6]:
            config = next((p.get("respuesta") for p in e.get("pasos", []) if p.get("respuesta")), {}) or {}
            fallas = [f"{p.get('paso')}:{p.get('error')}" for p in e.get("pasos", []) if p.get("resultado") not in (None, "OK")]
            inf(f"- {e.get('inicio')} {e.get('estado')} periodo={config.get('periodoNegocio')} lote={config.get('loteCargueId')} "
                f"ventana={config.get('fechaInicio')}→{config.get('fechaFin')} status={config.get('status')} fallas={fallas or '-'}")
    inf.capa("3. Orquestación", "IGUAL" if igual else "DIFIERE")


def _estados(definicion, salida=None):
    salida = {} if salida is None else salida
    for nombre, estado in (definicion.get("States") or {}).items():
        salida[nombre] = {k: v for k, v in estado.items() if k not in ("Branches", "Iterator", "ItemProcessor")}
        for rama in estado.get("Branches", []):
            _estados(rama, salida)
        for clave in ("Iterator", "ItemProcessor"):
            if clave in estado:
                _estados(estado[clave], salida)
    return salida


def comparar_configuracion(inf, a, b, na, nb):
    inf.titulo("4. Configuración (PRG, CNF y JSON publicados)")
    distintos = []
    pa = leer_json(os.path.join(a, "configuracion", "prg_programacion_emisiones.json"), {}) or {}
    pb = leer_json(os.path.join(b, "configuracion", "prg_programacion_emisiones.json"), {}) or {}
    inf(f"| PRG | {na} | {nb} |\n|---|---|---|")
    for clave in sorted(set(pa) | set(pb)):
        va, vb = json.dumps(pa.get(clave), ensure_ascii=False, sort_keys=True), json.dumps(pb.get(clave), ensure_ascii=False, sort_keys=True)
        marca = "" if va == vb else " **≠**"
        if clave in {"fechaActualizacion", "fechaCreacion", "ultimaEjecucion"} and va != vb:
            marca = " (fecha)"
        elif va != vb and clave not in {"idEjecucion", "loteCargueId"}:
            distintos.append(f"PRG.{clave}")
        inf(f"| {clave}{marca} | {va[:120]} | {vb[:120]} |")

    for caso in CASOS:
        ra = leer_json(os.path.join(a, "configuracion", f"resumen_{caso}.json"), {}) or {}
        rb = leer_json(os.path.join(b, "configuracion", f"resumen_{caso}.json"), {}) or {}
        ea, eb = (ra.get("extraccion") or {}), (rb.get("extraccion") or {})
        ca, cb = ea.get("contenido") or {}, eb.get("contenido") or {}
        inf(f"\n**{caso}** — configuración de extracción publicada")
        for clave in ("ventana_extraccion", "filtros_efectivos", "rutas", "fuente", "control_extraccion"):
            va = normalizar(json.dumps(ca.get(clave), sort_keys=True, ensure_ascii=False))
            vb = normalizar(json.dumps(cb.get(clave), sort_keys=True, ensure_ascii=False))
            if va != vb:
                distintos.append(f"{caso}.{clave}")
                inf(f"- {clave} **≠**: {na}={va[:300]} | {nb}={vb[:300]}")
        ta = {t["tabla_relativa"]: t for t in ea.get("tablas", [])}
        tb = {t["tabla_relativa"]: t for t in eb.get("tablas", [])}
        sql_a = _sql_normalizado(a, caso)
        sql_b = _sql_normalizado(b, caso)
        for tabla in sorted(set(ta) | set(tb)):
            if tabla not in ta or tabla not in tb:
                distintos.append(f"{caso}.{tabla}")
                inf(f"- tabla {tabla}: solo en {na if tabla in ta else nb}")
            elif sql_a.get(tabla) != sql_b.get(tabla):
                distintos.append(f"{caso}.{tabla}.sql")
                inf(f"- tabla {tabla}: **SQL distinta** (normalizando ambiente; fechas y lote cuentan como diferencia)")
        for tipo in ("validacion", "remediacion", "scd"):
            ha = _contenido_normalizado(a, caso, ra.get(tipo))
            hb = _contenido_normalizado(b, caso, rb.get(tipo))
            if ha != hb:
                distintos.append(f"{caso}.config_{tipo}")
                inf(f"- config de {tipo}: **distinta** (normalizando ambiente)")
    inf.capa("4. Configuración", "DIFIERE" if distintos else "IGUAL", ", ".join(distintos[:20]))


def _sql_normalizado(raiz, caso):
    salida = {}
    for nombre in os.listdir(os.path.join(raiz, "configuracion", "s3", caso)) if os.path.isdir(os.path.join(raiz, "configuracion", "s3", caso)) else []:
        if nombre.startswith("config_extraccion"):
            config = leer_json(os.path.join(raiz, "configuracion", "s3", caso, nombre), {})
            nucleo = config.get("nucleo_comun") or {}
            tablas = [t for f in nucleo.get("fases", []) for t in f.get("tablas", [])]
            tablas += [t for c in config.get("ciclos", []) for s in c.get("segmentos", []) for t in s.get("tablas", [])]
            for t in tablas:
                salida[t.get("tabla_relativa")] = normalizar(re.sub(r"\s+", " ", str(t.get("consulta_sql") or t.get("where") or "")))
    return salida


def _contenido_normalizado(raiz, caso, meta):
    if not meta or not meta.get("uri"):
        return None
    texto = leer_texto(os.path.join(raiz, "configuracion", "s3", caso, os.path.basename(meta["uri"])))
    return normalizar(texto) if texto is not None else None


def comparar_runtime(inf, a, b, na, nb):
    inf.titulo("5. Extracción (runtime parametros_extraccion.json)")
    distintos = []
    for caso in CASOS:
        ra = ((leer_json(os.path.join(a, "configuracion", f"resumen_{caso}.json"), {}) or {}).get("runtime") or {}).get("contenido") or {}
        rb = ((leer_json(os.path.join(b, "configuracion", f"resumen_{caso}.json"), {}) or {}).get("runtime") or {}).get("contenido") or {}
        if not ra and not rb:
            continue
        inf(f"\n**{caso}** — {na}: periodo={ra.get('periodo_negocio')} lote={ra.get('lote_cargue_id')} calidad={ra.get('calidad_fuentes')} "
            f"silver={ra.get('total_registros_silver_preparada')} | {nb}: periodo={rb.get('periodo_negocio')} lote={rb.get('lote_cargue_id')} "
            f"calidad={rb.get('calidad_fuentes')} silver={rb.get('total_registros_silver_preparada')}")
        da = {d.get("tabla_relativa"): d for d in ra.get("datasets", [])}
        db = {d.get("tabla_relativa"): d for d in rb.get("datasets", [])}
        inf(f"| Tabla | {na} estado | {na} filas | {nb} estado | {nb} filas | Dif |\n|---|---|---|---|---|---|")
        for tabla in sorted(set(da) | set(db), key=str):
            xa, xb = da.get(tabla, {}), db.get(tabla, {})
            fa, fb = xa.get("total_registros"), xb.get("total_registros")
            dif = (fb or 0) - (fa or 0) if isinstance(fa, int) and isinstance(fb, int) else "?"
            if dif not in (0, "?") or xa.get("estado") != xb.get("estado"):
                distintos.append(f"{caso}.{tabla}")
                inf(f"| {tabla} | {xa.get('estado')} | {fa} | {xb.get('estado')} | {fb} | **{dif}** |")
        if not any(d.startswith(caso + ".") for d in distintos):
            inf("| (todas las tablas) | | | | | igual |")
    inf.capa("5. Extracción", "DIFIERE" if distintos else "IGUAL", ", ".join(distintos[:20]))


def comparar_bronce(inf, a, b, na, nb):
    inf.titulo("6. Bronce: movimientos por producto y concepto")
    distintos, hubo = [], False
    for caso in CASOS:
        fa = leer_csv(os.path.join(a, "datos", f"bronce_{caso}_movimientos.csv"))
        fb = leer_csv(os.path.join(b, "datos", f"bronce_{caso}_movimientos.csv"))
        if fa is None or fb is None:
            continue
        hubo = True
        ma = {(f["productid"], f["concepto"]): (dec(f["filas"]), dec(f["total"])) for f in fa}
        mb = {(f["productid"], f["concepto"]): (dec(f["filas"]), dec(f["total"])) for f in fb}
        lineas = []
        for llave in sorted(set(ma) | set(mb)):
            xa, xb = ma.get(llave, (0, Decimal(0))), mb.get(llave, (0, Decimal(0)))
            if xa != xb:
                lineas.append(f"| {llave[0]} | {llave[1]} | {xa[0]} | {xa[1]:.2f} | {xb[0]} | {xb[1]:.2f} | {xb[1] - xa[1]:.2f} |")
        inf(f"\n**{caso}**: {len(lineas)} combinaciones producto-concepto distintas de {len(set(ma) | set(mb))}")
        if lineas:
            distintos.append(caso)
            inf(f"| Producto | Concepto | {na} filas | {na} total | {nb} filas | {nb} total | Dif |\n|---|---|---|---|---|---|---|")
            inf("\n".join(lineas[:60]))
    inf.capa("6. Bronce", "SIN DATOS" if not hubo else ("DIFIERE" if distintos else "IGUAL"), ", ".join(distintos))


def totales_silver(raiz, caso, archivo="totales"):
    filas = leer_csv(os.path.join(raiz, "datos", f"silver_{caso}_{archivo}.csv"))
    if filas is None:
        return None
    columna = next((c for c in ("idproducto", "id_producto", "productid", "codigo_producto") if filas and c in filas[0]), None)
    salida = defaultdict(lambda: defaultdict(Decimal))
    for fila in filas:
        producto = str(fila.get(columna)).split(".")[0] if columna else "(todos)"
        for clave, valor in fila.items():
            if clave in {columna, "periodo_fechaemision", "lote_cargue_ids", "id_ejecucions", "ejecucion_preparacion"}:
                continue
            salida[producto][clave] += dec(valor)
    return salida


def comparar_silver(inf, a, b, na, nb):
    inf.titulo("7. Silver preparada: totales por producto")
    distintos, hubo = [], False
    for caso in CASOS:
        ta, tb = totales_silver(a, caso), totales_silver(b, caso)
        if ta is None or tb is None:
            continue
        hubo = True
        inf(f"\n**{caso}**")
        inf(f"| Producto | Columna | {na} | {nb} | Dif |\n|---|---|---|---|---|")
        for producto in sorted(set(ta) | set(tb)):
            for columna in sorted(set(ta.get(producto, {})) | set(tb.get(producto, {}))):
                va, vb = ta.get(producto, {}).get(columna, Decimal(0)), tb.get(producto, {}).get(columna, Decimal(0))
                if va.quantize(CENTAVO) != vb.quantize(CENTAVO):
                    distintos.append(f"{caso}.{producto}.{columna}")
                    inf(f"| {producto} | {columna} | {va:.2f} | {vb:.2f} | {vb - va:.2f} |")
        for etiqueta, raiz, total in ((na, a, ta), (nb, b, tb)):
            llave = totales_silver(raiz, caso, "totales_por_operacion")
            if not llave:
                continue
            for producto, columnas in sorted(llave.items()):
                filas, llaves = total.get(producto, {}).get("filas", 0), columnas.get("llaves", 0)
                duplicadas = [c for c in columnas if c not in {"llaves", "filas"}
                              and columnas[c].quantize(CENTAVO) != total.get(producto, {}).get(c, Decimal(0)).quantize(CENTAVO)]
                if duplicadas:
                    inf(f"- {etiqueta} {caso} {producto}: {filas} filas para {llaves} llaves (operación, póliza); "
                        f"sumar filas da otro total en {len(duplicadas)} columnas (ej. {duplicadas[:4]}) → **montos repetidos por fila**")
    inf.capa("7. Silver", "SIN DATOS" if not hubo else ("DIFIERE" if distintos else "IGUAL"), f"{len(distintos)} valores distintos")


def comparar_con_excel(inf, raiz, etiqueta, referencia, mapeo):
    inf.titulo(f"Silver de {etiqueta} frente al Excel (mapeo PROPUESTO)")
    if not referencia or not mapeo:
        inf("Sin referencia o sin mapeo.")
        return
    bloques = {
        "generales": [("mes_produccion_periodo", referencia["generales"]["mes_produccion_periodo"]),
                      ("todo_el_corte", referencia["generales"]["todo_el_corte"])],
        "satelites": [("resumen_satelites", referencia.get("satelites", {}))],
        "pcr_vida": [("resumen_vida", {k: v for k, v in referencia.get("vida", {}).items() if k == "48897"})],
    }
    for caso, variantes in bloques.items():
        for archivo in ("totales", "totales_por_operacion"):
            silver = totales_silver(raiz, caso, archivo)
            if silver is None:
                continue
            for variante, esperado in variantes:
                inf(f"\n**{caso}** ({archivo}) frente a `{variante}`")
                inf("| Producto | Columna Excel | Excel | Silver | Dif | Columnas Silver |\n|---|---|---|---|---|---|")
                for producto, metricas in sorted(esperado.items()):
                    for metrica, columnas in mapeo.get(caso, {}).items():
                        if metrica not in metricas:
                            continue
                        valor_excel = Decimal(metricas[metrica])
                        if not columnas:
                            inf(f"| {producto} | {metrica} | {valor_excel:.2f} | — | — | el Silver no produce este valor |")
                            continue
                        valor_silver = sum((silver.get(producto, {}).get(c, Decimal(0)) for c in columnas), Decimal(0))
                        dif = valor_silver - valor_excel
                        marca = "OK" if abs(dif) < 1 else f"**{dif:.2f}**"
                        inf(f"| {producto} | {metrica} | {valor_excel:.2f} | {valor_silver:.2f} | {marca} | {'+'.join(columnas)} |")


def alertas(inf, raiz, etiqueta):
    resumen = leer_json(os.path.join(raiz, "resumen.json"), {}) or {}
    inf.titulo(f"Alertas automáticas de {etiqueta}")
    for alerta in resumen.get("alertas", []):
        extra = {k: v for k, v in alerta.items() if k not in {"codigo", "detalle"}}
        inf(f"- `{alerta['codigo']}` {alerta['detalle']} {json.dumps(extra, ensure_ascii=False) if extra else ''}")
    for error in resumen.get("errores", []):
        inf(f"- ERROR de captura en `{error['seccion']}`: {error['error'][:200]}")


def main():
    parser = argparse.ArgumentParser(description="Compara dos evidencias del diagnóstico de Emisiones")
    parser.add_argument("--a", required=True)
    parser.add_argument("--b", required=True)
    parser.add_argument("--referencia")
    parser.add_argument("--mapeo")
    parser.add_argument("--salida", default="informe-comparacion.md")
    args = parser.parse_args()
    na = (leer_json(os.path.join(args.a, "resumen.json"), {}) or {}).get("ambiente", "A")
    nb = (leer_json(os.path.join(args.b, "resumen.json"), {}) or {}).get("ambiente", "B")
    referencia = leer_json(args.referencia) if args.referencia else None
    mapeo = leer_json(args.mapeo) if args.mapeo else None

    inf = Informe()
    inf(f"# Comparación de Emisiones: {na} frente a {nb}")
    comparar_oracle(inf, args.a, args.b, na, nb)
    comparar_codigo(inf, args.a, args.b, na, nb)
    comparar_orquestacion(inf, args.a, args.b, na, nb)
    comparar_configuracion(inf, args.a, args.b, na, nb)
    comparar_runtime(inf, args.a, args.b, na, nb)
    comparar_bronce(inf, args.a, args.b, na, nb)
    comparar_silver(inf, args.a, args.b, na, nb)
    comparar_con_excel(inf, args.a, na, referencia, mapeo)
    comparar_con_excel(inf, args.b, nb, referencia, mapeo)
    alertas(inf, args.a, na)
    alertas(inf, args.b, nb)

    veredicto = ["# Veredicto por capa", "", "| Capa | Resultado | Detalle |", "|---|---|---|"]
    for capa, (estado, detalle) in inf.capas.items():
        veredicto.append(f"| {capa} | {estado} | {detalle[:200]} |")
    primera = next((c for c, (e, _) in inf.capas.items() if e == "DIFIERE"), None)
    sin_datos = [c for c, (e, _) in inf.capas.items() if e == "SIN DATOS"]
    veredicto.append("")
    veredicto.append(f"**Primera capa con diferencia:** {primera or 'ninguna de las comparadas'}."
                     + (f" Sin comparar: {', '.join(sin_datos)}." if sin_datos else ""))
    texto = "\n".join(veredicto) + "\n" + inf.texto() + "\n"
    with open(args.salida, "w", encoding="utf-8") as archivo:
        archivo.write(texto)
    print("\n".join(veredicto))
    print(f"\nInforme completo: {args.salida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
