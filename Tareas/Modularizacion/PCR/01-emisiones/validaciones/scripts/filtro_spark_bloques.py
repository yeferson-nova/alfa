#!/usr/bin/env python3
# Genera los bloques contextos_spark y filtro_spark desde las consulta_sql de un JSON de extraccion de Emisiones.
#   python3 filtro_spark_bloques.py generar config_extraccion_generales.json [--modo SOMBRA] [--max-filas-oracle 1000000]
#   python3 filtro_spark_bloques.py aplicar config.json bloques.json salida.json
# Solo arma el bloque cuando la tabla se une directo al contexto (T.col = U.col) y el contexto es identico
# al de otras tablas del caso; el resto queda en el reporte con el motivo.
import json
import os
import re
import sys
from collections import OrderedDict

PIVOTE = "CONTEXTOPERATION"
HINT = re.compile(r"/\*\+.*?\*/", re.S)
TABLA = re.compile(r"^SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM\s+(\{SCHEMA\}|ACSELE_ALFA)\.(\w+)\s+(\w+)\s+WHERE\s+(.*)$",
                   re.S | re.I)
EXISTS = re.compile(r"^EXISTS\s*\(\s*SELECT\s+1\s+FROM\s*\(", re.I)
CRUCE = re.compile(r"^(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)$")
MODOS = ("SOMBRA", "ACTIVO")
TAMANOS = "tamanos-oracle-acsele.json"
MAX_FILAS_ORACLE = 1000000


def limpiar(sql):
    return re.sub(r"\s+", " ", HINT.sub(" ", sql or "")).strip()


def firma(sql):
    return limpiar(sql).upper()


def cierre(texto, inicio):
    nivel, en_texto = 0, False
    for i in range(inicio, len(texto)):
        ch = texto[i]
        if ch == "'":
            en_texto = not en_texto
        elif en_texto:
            continue
        elif ch == "(":
            nivel += 1
        elif ch == ")":
            nivel -= 1
            if nivel == 0:
                return i
    return -1


def partir_and(texto):
    partes, nivel, en_texto, ultimo, i = [], 0, False, 0, 0
    mayus = texto.upper()
    while i < len(texto):
        ch = texto[i]
        if ch == "'":
            en_texto = not en_texto
        elif not en_texto:
            if ch == "(":
                nivel += 1
            elif ch == ")":
                nivel -= 1
            elif nivel == 0 and mayus.startswith(" AND ", i):
                partes.append(texto[ultimo:i].strip())
                ultimo = i + 5
                i += 4
        i += 1
    partes.append(texto[ultimo:].strip())
    return [p for p in partes if p]


def quitar_parentesis(texto):
    texto = texto.strip()
    while texto.startswith("(") and cierre(texto, 0) == len(texto) - 1:
        texto = texto[1:-1].strip()
    return texto


def analizar(sql):
    texto = limpiar(sql)
    m = TABLA.match(texto)
    if not m:
        return None, "la consulta no tiene la forma SELECT ... FROM tabla alias WHERE ..."
    distinct, proyeccion, esquema, tabla, alias, where = m.groups()
    if " JOIN " in f" {proyeccion.upper()} ":
        return None, "la consulta principal tiene JOIN"
    condiciones = [quitar_parentesis(p) for p in partir_and(where)]
    existentes = [p for p in condiciones if PIVOTE in p.upper()]
    propias = [p for p in condiciones if PIVOTE not in p.upper()]
    if len(existentes) != 1:
        return None, f"el pivote aparece en {len(existentes)} condiciones"
    existe = existentes[0]
    if not EXISTS.match(existe):
        return None, "el pivote no esta en un EXISTS (SELECT 1 FROM (contexto) alias WHERE ...)"
    abre = existe.index("(", existe.upper().index("FROM"))
    fin = cierre(existe, abre)
    contexto = existe[abre + 1:fin].strip()
    if not contexto.upper().startswith("SELECT") or PIVOTE not in contexto.upper():
        return None, "el contexto no es una subconsulta con el pivote"
    resto = existe[fin + 1:].strip()
    m2 = re.match(r"^(\w+)\s+WHERE\s+(.*)\)$", resto, re.S | re.I)
    if not m2:
        return None, "el cruce pasa por una tabla intermedia o un JOIN"
    alias_ctx, cruce = m2.groups()
    mapeo = []
    for parte in partir_and(cruce):
        c = CRUCE.match(quitar_parentesis(parte))
        if not c:
            return None, f"condicion de cruce no directa: {parte}"
        a1, c1, a2, c2 = c.groups()
        lados = {a1.upper(): c1.upper(), a2.upper(): c2.upper()}
        if set(lados) != {alias.upper(), alias_ctx.upper()}:
            return None, f"condicion de cruce con otro alias: {parte}"
        mapeo.append({"origen": lados[alias_ctx.upper()], "destino": lados[alias.upper()]})
    columnas = [x.strip().upper() for x in proyeccion.split(",")]
    if columnas != [f"{alias.upper()}.*"]:
        faltan = [m["destino"] for m in mapeo if f"{alias.upper()}.{m['destino']}" not in columnas]
        if faltan:
            return None, f"la proyeccion no incluye la llave del cruce: {faltan}"
    for propia in propias:
        if "SELECT" in propia.upper():
            return None, f"filtro propio con subconsulta: {propia[:80]}"
    lectura = f"SELECT {'DISTINCT ' if distinct else ''}{proyeccion} FROM {esquema}.{tabla} {alias}"
    if propias:
        lectura += " WHERE " + " AND ".join(propias)
    return {
        "tabla": tabla.upper(),
        "contexto": contexto,
        "lectura": lectura,
        "mapeo": mapeo,
        "distinct": bool(distinct),
    }, None


def tablas_de_rama(config):
    for ciclo in config.get("ciclos", []):
        for segmento in ciclo.get("segmentos", []):
            for tabla in segmento.get("tablas", []):
                yield tabla


def nombre_contexto(contexto, tabla, usados):
    origen = re.search(r"\bFROM\s+(?:\{SCHEMA\}|ACSELE_ALFA)\.(\w+)", contexto, re.I)
    if origen and origen.group(1).upper() == PIVOTE:
        base = "negocio_con_prepolicy" if "PREPOLICY" in contexto.upper() else "negocio"
    else:
        base = f"subconsulta_{tabla.lower()}"
    nombre, n = base, 2
    while nombre in usados:
        nombre, n = f"{base}_{n}", n + 1
    return nombre


def generar(config, tamanos, modo="SOMBRA", max_filas_oracle=MAX_FILAS_ORACLE):
    contextos, nombres, bloques, reporte = OrderedDict(), {}, OrderedDict(), []
    for tabla in tablas_de_rama(config):
        relativa = str(tabla.get("tabla_relativa", "")).strip("/").lower()
        nombre = str(tabla.get("tabla", relativa.split("/")[-1])).upper()
        filas = tamanos.get(nombre)
        fila = {"caso": config.get("nombre_caso"), "tabla_relativa": relativa, "tabla": nombre, "filas_oracle": filas}
        if tabla.get("estrategia_lectura", "SQL_CONFIGURADA") not in ("SQL_CONFIGURADA", "SQL_CONFIGURADA_UNA_TABLA"):
            reporte.append({**fila, "decision": "SIN_BLOQUE", "motivo": "estrategia distinta de SQL_CONFIGURADA"})
            continue
        if filas is None or filas > max_filas_oracle:
            reporte.append({**fila, "decision": "SIN_BLOQUE", "motivo": f"tabla sin tamano o con mas de {max_filas_oracle} filas en Oracle"})
            continue
        resultado, motivo = analizar(tabla.get("consulta_sql", ""))
        if not resultado:
            reporte.append({**fila, "decision": "SIN_BLOQUE", "motivo": motivo})
            continue
        clave = firma(resultado["contexto"])
        if clave not in nombres:
            nombres[clave] = nombre_contexto(resultado["contexto"], nombre, set(nombres.values()))
            contextos[nombres[clave]] = {"consulta_sql": resultado["contexto"]}
        contexto = nombres[clave]
        bloques[relativa] = {
            "modo": modo,
            "consulta_lectura": resultado["lectura"],
            "cruces": [{"origen": f"contexto.{contexto}", "mapeo": resultado["mapeo"]}],
            "distinct": resultado["distinct"],
        }
        reporte.append({**fila, "decision": "BLOQUE", "motivo": "", "contexto": contexto,
                        "cruce": " AND ".join(f"{m['destino']}=U.{m['origen']}" for m in resultado["mapeo"])})
    usados = {b["cruces"][0]["origen"].split(".", 1)[1] for b in bloques.values()}
    contextos = OrderedDict((k, v) for k, v in contextos.items() if k in usados)
    return {"contextos_spark": contextos, "tablas": bloques}, reporte


def aplicar(config, bloques):
    salida = json.loads(json.dumps(config))
    if bloques["contextos_spark"]:
        salida["contextos_spark"] = bloques["contextos_spark"]
    aplicadas = 0
    for tabla in tablas_de_rama(salida):
        relativa = str(tabla.get("tabla_relativa", "")).strip("/").lower()
        if relativa in bloques["tablas"]:
            tabla["filtro_spark"] = bloques["tablas"][relativa]
            aplicadas += 1
    return salida, aplicadas


def cargar_tamanos(ruta=None):
    ruta = ruta or os.path.join(os.path.dirname(os.path.abspath(__file__)), TAMANOS)
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def _opcion(args, nombre, defecto):
    return args[args.index(nombre) + 1] if nombre in args else defecto


def main(args):
    if not args or args[0] not in ("generar", "aplicar"):
        print(__doc__ or "uso: generar <config.json> | aplicar <config.json> <bloques.json> <salida.json>")
        return 2
    if args[0] == "aplicar":
        config = json.load(open(args[1], encoding="utf-8"))
        bloques = json.load(open(args[2], encoding="utf-8"))
        salida, aplicadas = aplicar(config, bloques)
        json.dump(salida, open(args[3], "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"tablas con bloque: {aplicadas}")
        return 0
    modo = _opcion(args, "--modo", "SOMBRA").upper()
    if modo not in MODOS:
        print(f"modo no valido: {modo}")
        return 2
    maximo = int(_opcion(args, "--max-filas-oracle", MAX_FILAS_ORACLE))
    tamanos = cargar_tamanos(_opcion(args, "--tamanos", None))
    for ruta in [a for a in args[1:] if a.endswith(".json") and a != _opcion(args, "--tamanos", None)]:
        config = json.load(open(ruta, encoding="utf-8"))
        bloques, reporte = generar(config, tamanos, modo, maximo)
        caso = config.get("nombre_caso", os.path.basename(ruta))
        destino = f"filtro-spark-{caso}.json"
        json.dump(bloques, open(destino, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        con = [r for r in reporte if r["decision"] == "BLOQUE"]
        print(f"== {caso}: {len(con)} tablas con bloque de {len(reporte)} | contextos: {list(bloques['contextos_spark'])} -> {destino}")
        for r in reporte:
            print(f"   {r['decision']:10s} {r['tabla']:32s} {str(r['filas_oracle']):>12s}  {r.get('cruce') or r['motivo']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
