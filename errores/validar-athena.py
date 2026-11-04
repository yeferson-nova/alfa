"""Valida con el resultado REAL de Athena (prod) la deduccion pendiente del analisis.

Entrada: CSV de la consulta
    SELECT act_reg, per_proceso_point, COUNT(*) AS registros
    FROM "niif-prd-gold-catalog-db"."scd_primas_arl" GROUP BY 1, 2

Comprueba contra las metricas del SCD (parametros_extraccion) y contra el periodo 202606.

Uso: python -I validar-athena.py <carpeta evidencia-primas-arl-202606>
"""
import csv
import json
import sys
from pathlib import Path

if len(sys.argv) != 2:
    sys.exit("Uso: python -I validar-athena.py <carpeta-evidencia>")
base = Path(sys.argv[1])
archivos = sorted((base / "athena").glob("*.csv"))
if not archivos:
    sys.exit("No hay CSV en athena/")

filas = []
with open(archivos[0], newline="", encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        filas.append((int(r["act_reg"]), r["per_proceso_point"] or None, int(r["registros"])))

m = json.load(open(base / "s3/parametros_extraccion_primas_arl.json", encoding="utf-8"))["metricas_gold_scd"]

act = [x for x in filas if x[0] == 1]
ina = [x for x in filas if x[0] == 0]
total_act = sum(x[2] for x in act)
total_ina = sum(x[2] for x in ina)
act_202606 = sum(x[2] for x in act if x[1] == "202606")
ina_202606 = sum(x[2] for x in ina if x[1] == "202606")
sin_periodo_ina = sum(x[2] for x in ina if x[1] is None)
resultados = []


def verificar(nombre, ok, detalle):
    resultados.append(ok)
    print(("PASS " if ok else "FAIL ") + nombre + "\n     " + detalle)


print(f"CSV: {archivos[0].name}\n")
verificar("Gold tiene solo 209 registros activos (act_reg = 1)",
          total_act == 209 == m["total_insertados_nuevos"], f"activos={total_act} insertados_scd={m['total_insertados_nuevos']}")
verificar("NINGUN activo es del periodo 202606 (por eso 'Totales' = No hay datos)",
          act_202606 == 0, f"activos con per_proceso_point=202606: {act_202606}")
verificar("Total de filas del CSV == total_gold_final del SCD",
          total_act + total_ina == m["total_gold_final"], f"{total_act + total_ina:,} == {m['total_gold_final']:,}")
verificar("Inactivos del CSV == Gold anterior (historicos previos + apagados)",
          total_ina == m["total_gold_anterior"], f"{total_ina:,} == {m['total_gold_anterior']:,}")

print("\nDetalle de lo que quedo activo:")
for a, p, n in act:
    print(f"   per_proceso_point={p or 'NULO'}: {n}")
print(f"\nHallazgos para el informe:")
print(f"   - El periodo 202606 tiene {ina_202606:,} registros INACTIVOS (act_reg=0) y 0 activos.")
print(f"     Un 'Totales' correcto deberia mostrar hasta {ina_202606:,} registros (cota superior: algunos pudieron ser historicos).")
print(f"   - {sin_periodo_ina:,} registros inactivos tienen per_proceso_point NULO "
      f"({sin_periodo_ina / (total_act + total_ina):.1%} de Gold): nunca aparecen en la certificacion de ningun periodo.")
print(f"\n{sum(resultados)}/{len(resultados)} verificaciones PASS")
sys.exit(0 if all(resultados) else 1)
