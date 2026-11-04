"""Valida con código el análisis del incidente Primas ARL 202606 (prod).

Lee la evidencia recolectada en CloudShell (solo lectura) y comprueba cada
afirmación del análisis contra los archivos. No se supone nada: cada aserción
imprime PASS o FAIL con los valores reales.

Uso:
    python -I validar-analisis.py <carpeta evidencia-primas-arl-202606>
"""
import json
import sys
from pathlib import Path

if len(sys.argv) != 2:
    sys.exit("Uso: python -I validar-analisis.py <carpeta-evidencia>")
base = Path(sys.argv[1])


def cargar(ruta):
    with open(base / ruta, encoding="utf-8") as f:
        return json.load(f)


par = cargar("s3/parametros_extraccion_primas_arl.json")
caso = cargar("s3/config_caso_1_primasArl.json")
cfg_ext = cargar("s3/config_extraccion_primas_arl.json")
grupos = cargar("s3/composite_groups.json")
ejec = cargar("stepfunction/ejecuciones-recientes.json")["executions"]
estado = cargar("stepfunction/01-estado-y-entrada.json")
resueltos = cargar("stepfunction/05-configKey-resueltos.json")

resultados = []


def verificar(nombre, condicion, detalle):
    resultados.append(condicion)
    print(("PASS " if condicion else "FAIL ") + nombre + "\n     " + detalle)


m = par["metricas_gold_scd"]
endosos = next(d for d in par["datasets_extraidos"] if d["tabla_relativa"] == "arpprod/par_endosos")

print("== A. El flujo terminó bien (no es una falla técnica) ==")
verificar("Step Function SUCCEEDED", estado["status"] == "SUCCEEDED", f"status={estado['status']}")
estados_flujo = {k: par.get(k) for k in (
    "estado_extraccion", "estado_remediacion", "estado_disponibilizacion_silver",
    "estado_preparacion_silver", "estado_gold_scd")}
verificar("Extracción, remediación, Silver y Gold en SUCCESS",
          all(v == "SUCCESS" for v in estados_flujo.values()), str(estados_flujo))

print("\n== B. Resumen vacío = configuración ==")
verificar("queries.summary está vacío en el JSON del caso",
          caso["queries"]["summary"] == [], f"summary={caso['queries']['summary']}")
verificar("queries.total tiene 1 consulta y detail tiene 1",
          len(caso["queries"]["total"]) == 1 and len(caso["queries"]["detail"]) == 1,
          f"total={len(caso['queries']['total'])} detail={len(caso['queries']['detail'])}")
verificar("Caso simple: primasArl no pertenece a un grupo compuesto",
          "primasArl" not in json.dumps(grupos) and resueltos[0]["notificationMode"] == "SIMPLE",
          f"notificationMode={resueltos[0]['notificationMode']}")

print("\n== C. La extracción de esta corrida fue incremental ==")
verificar("El periodo 202606 ya se había ejecutado antes (hay otra ejecución previa)",
          sum(1 for e in ejec if e["name"].startswith("arl-202606")) >= 2,
          ", ".join(f"{e['name']} {e['startDate'][:10]}" for e in ejec))
verificar("Modo de segmentación EXCLUIR_EXTRAIDOS sobre NID_ENDOSO",
          endosos["segmentacion_horizontal"]["modo_segmentacion"] == "EXCLUIR_EXTRAIDOS"
          and endosos["segmentacion_horizontal"]["llaves_segmentacion"] == ["NID_ENDOSO"],
          f"modo={endosos['segmentacion_horizontal']['modo_segmentacion']}")
verificar("El WHERE de par_endosos es acumulado (PERIODO <= 202606), no solo el mes",
          "PERIODO <= '202606'" in json.dumps(cfg_ext),
          "where \"PERIODO <= '202606'\" en config_extraccion_primas_arl.json")
verificar("Solo 209 endosos nuevos extraídos; 1.203.789 excluidos por ya extraídos",
          endosos["total_registros"] == 209 and endosos["segmentacion_horizontal"]["registros_excluidos_por_segmento"] == 1203789,
          f"extraidos={endosos['total_registros']} excluidos={endosos['segmentacion_horizontal']['registros_excluidos_por_segmento']}")
verificar("Silver preparada quedó solo con esos 209 y se escribe con overwrite",
          par["total_registros_silver_preparada"] == 209 and par["modo_escritura_silver_preparada"] == "overwrite",
          f"silver={par['total_registros_silver_preparada']} modo={par['modo_escritura_silver_preparada']}")

print("\n== D. El SCD de Gold apagó todo lo que no vino en esa corrida ==")
verificar("Entrada del SCD = 209 (la foto parcial)", m["total_entrada_silver_preparada"] == 209, f"entrada={m['total_entrada_silver_preparada']}")
verificar("Apagados por ausencia == excluidos por segmento (1.203.789)",
          m["total_registros_apagados_por_ausencia"] == endosos["segmentacion_horizontal"]["registros_excluidos_por_segmento"],
          f"apagados={m['total_registros_apagados_por_ausencia']} excluidos={endosos['segmentacion_horizontal']['registros_excluidos_por_segmento']}")
activos_previos = (m["total_registros_apagados_por_ausencia"] + m["total_registros_sin_cambio"]
                   + m["total_versiones_cerradas_por_cambio"])
verificar("Ninguno de los activos previos coincidió con la entrada (sin cambio=0, cerradas=0)",
          m["total_registros_sin_cambio"] == 0 and m["total_versiones_cerradas_por_cambio"] == 0,
          f"sin_cambio={m['total_registros_sin_cambio']} cerradas={m['total_versiones_cerradas_por_cambio']}")
activos_despues = m["total_insertados_nuevos"] + m["total_registros_sin_cambio"] + m["total_nuevas_versiones_por_cambio"]
verificar("Gold final = Gold anterior + insertados (consistencia de métricas)",
          m["total_gold_final"] == m["total_gold_anterior"] + m["total_insertados_nuevos"],
          f"{m['total_gold_anterior']} + {m['total_insertados_nuevos']} = {m['total_gold_final']}")
print(f"     DEDUCIDO: activos (act_reg=1) antes = {activos_previos:,}; "
      f"históricos antes = {m['total_gold_anterior'] - activos_previos:,}; "
      f"activos (act_reg=1) DESPUÉS = {activos_despues:,}")
verificar("Tras el SCD solo quedan 209 registros activos en Gold",
          activos_despues == 209, f"activos_despues={activos_despues}")

print("\n== E. Por qué los totales y el CSV salen vacíos ==")
q_total = caso["queries"]["total"][0]["query"]
q_detail = caso["queries"]["detail"][0]["query"]
verificar("La consulta total filtra act_reg = 1 y per_proceso_point = '$period'",
          "act_reg = 1" in q_total and "per_proceso_point AS VARCHAR) = '$period'" in q_total, "ver total#1")
verificar("El detalle filtra igual y SIEMPRE incluye fila de encabezado (UNION ALL, orden 0)",
          "act_reg = 1" in q_detail and "UNION ALL" in q_detail and "0 AS orden" in q_detail,
          "por eso el CSV existe y sinDatosPeriodo no se activa aunque no haya filas")

print("\n== Resultado ==")
print(f"{sum(resultados)}/{len(resultados)} verificaciones PASS")
sys.exit(0 if all(resultados) else 1)
