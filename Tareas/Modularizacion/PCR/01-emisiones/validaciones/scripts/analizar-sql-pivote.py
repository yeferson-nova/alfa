import csv
import hashlib
import json
import re
import sys
import warnings
from collections import Counter, defaultdict
from pathlib import Path

warnings.filterwarnings("ignore")
REPO = Path(sys.argv[1])
SALIDAS = Path(sys.argv[2])
OUT = Path(sys.argv[3])
sys.path.insert(0, str(REPO / "src"))

import openpyxl
from emissions_extraction.domain import configuration as cf
from emissions_extraction.domain.naming import table_name, table_relative
from emissions_extraction.domain.period import month_window
from emissions_extraction.domain.sql_compiler import compile_sql, strip_oracle_hints

CASES = ("pcr_vida", "generales", "satelites")
LOTE = "20261124060016"


def norm(sql):
    sql = strip_oracle_hints(sql or "")
    sql = re.sub(r"--[^\n]*", " ", sql)
    return re.sub(r"\s+", " ", sql).strip().upper()


def enclosing_select(sql, pos):
    depth = 0
    i = pos
    while i >= 0:
        ch = sql[i]
        if ch == ")":
            depth += 1
        elif ch == "(":
            if depth == 0:
                if sql[i + 1:i + 8].startswith("SELECT"):
                    return i
            else:
                depth -= 1
        i -= 1
    return None


def matching(sql, start):
    depth = 0
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return None


def context_info(sql):
    pos = sql.find("CONTEXTOPERATION")
    if pos < 0:
        return None
    start = enclosing_select(sql, pos)
    if start is None:
        return {"kind": "SIN_SUBCONSULTA"}
    end = matching(sql, start)
    inner = sql[start + 1:end]
    from_part = inner[inner.find(" FROM "):]
    joins = re.findall(r"(?:FROM|JOIN) \{SCHEMA\}\.(\w+)|(?:FROM|JOIN) ACSELE_ALFA\.(\w+)", from_part)
    joined = [a or b for a, b in joins]
    after = sql[end + 1:end + 260]
    return {
        "kind": "SUBCONSULTA",
        "inner_sig": hashlib.sha1(from_part.encode()).hexdigest()[:8],
        "inner_tables": "+".join(joined),
        "after": after,
        "inner": inner,
    }


def main_table(sql):
    m = re.search(r"FROM (?:\{SCHEMA\}|ACSELE_ALFA)\.(\w+)(?: (\w+))?", sql)
    return m.group(1) if m else ""


def correlation(after):
    m = re.match(r"\s*(\w+)\s+(ON .*?)?\s*WHERE (.*?)\)", after)
    if not m:
        return after[:120]
    return ((m.group(2) or "") + " WHERE " + m.group(3)).strip()


def load_times():
    times = {}
    text = (SALIDAS / "medir-tiempos-dev-20261125-2348.txt").read_text(encoding="utf-8")
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 7 and parts[1] == LOTE:
            caso, _, escr, _, filas, estado, tabla = parts[:7]
            times[(caso, tabla)] = (int(escr), int(filas), estado)
    return times


def load_sizes():
    wb = openpyxl.load_workbook(SALIDAS / "consulta2-tamanos.xlsx")
    rows = list(wb.worksheets[0].iter_rows(values_only=True))
    head = rows[0]
    return {r[1]: dict(zip(head, r)) for r in rows[1:] if r[1]}


def load_index():
    wb = openpyxl.load_workbook(SALIDAS / "consulta1-llaves-indices.xlsx")
    rows = list(wb.worksheets[0].iter_rows(values_only=True))
    return {(r[1], r[2]): r[8] for r in rows[1:] if r[1]}


times, sizes, indexes = load_times(), load_sizes(), load_index()
rows = []
for case in CASES:
    raw = json.loads((REPO / f"tests/fixtures/config/config_extraccion_{case}.json").read_text(encoding="utf-8"))
    config = cf.normalize_config(raw, f"s3://x/{case}.json")
    start, end = month_window(config["periodo_negocio"])
    mapping = cf.table_map(config)
    for scope, tables in (("nucleo", cf.flatten_core_tables(config)), ("rama", cf.flatten_branch_tables(config))):
        for table in tables:
            sql = norm(compile_sql(table, mapping, config, start, end))
            ctx = context_info(sql) or {"kind": "SIN_PIVOTE"}
            name = table_name(table)
            rel = table_relative(table)
            time_key = ("comun" if scope == "nucleo" else case, rel)
            escr, filas, estado = times.get(time_key, (None, None, None))
            size = sizes.get(name.upper(), {})
            rows.append({
                "caso": case, "alcance": scope, "fase": table["fase"], "modo": table["modo_fase"],
                "tabla": name, "estrategia": table.get("estrategia_lectura", "SQL_CONFIGURADA"),
                "pivote_veces": sql.count("CONTEXTOPERATION"),
                "patron": ctx["kind"], "contexto_firma": ctx.get("inner_sig", ""),
                "contexto_tablas": ctx.get("inner_tables", ""),
                "correlacion": correlation(ctx["after"]) if ctx.get("after") is not None else "",
                "distinct": " DISTINCT " in sql[:40], "with": sql.startswith("WITH"),
                "tabla_principal": main_table(sql),
                "escritura_s": escr, "filas_lote": filas, "estado_lote": estado,
                "num_rows_oracle": size.get("NUM_ROWS"), "particionada": size.get("PARTITIONED"),
                "sql_len": len(sql),
            })

with OUT.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

print("total filas", len(rows))
print("por caso/alcance", Counter((r["caso"], r["alcance"]) for r in rows))
print("con pivote", sum(1 for r in rows if r["pivote_veces"]), "sin", sum(1 for r in rows if not r["pivote_veces"]))
print("patron", Counter((r["caso"], r["patron"]) for r in rows))
print("veces pivote", Counter(r["pivote_veces"] for r in rows))
sig = defaultdict(list)
for r in rows:
    if r["contexto_firma"]:
        sig[(r["caso"], r["contexto_firma"], r["contexto_tablas"])].append(r["tabla"])
for k, v in sorted(sig.items(), key=lambda x: -len(x[1])):
    print("FIRMA", k, len(v), v[:6])
