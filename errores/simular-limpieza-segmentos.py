"""Simula la logica del SCD (Backend/Glue/niif-dev-scd/scd.py, lineas 335-437) para
evaluar la propuesta "limpiar los segmentos de Primas ARL y volver a correr".

Usa los numeros reales de prod (evidencia 202606). Cada llave es un NID_ENDOSO; se
asume que una llave existente conserva el mismo HASH (sin cambios de contenido).
Un registro de Gold es (llave, activo, version). Es una simulacion de la LOGICA, no
una ejecucion sobre datos reales.

Uso: python -I simular-limpieza-segmentos.py
"""
N = 1_203_789        # activos en Gold antes del 2 nov (= excluidos por segmento)
H = 3_454            # historicos antes (1.207.243 - 1.203.789)
K = 209              # endosos nuevos de la corrida del 2 nov


def scd(gold, entrada):
    """gold: lista (llave, act, vrs). entrada: set de llaves. Devuelve (gold_nuevo, metricas)."""
    activos = {k: v for k, a, v in gold if a == 1}
    hist = [r for r in gold if r[1] == 0]
    insertar = [k for k in entrada if k not in activos]          # nuevos
    apagar = [k for k in activos if k not in entrada]            # ausentes
    sin_cambio = [k for k in entrada if k in activos]            # mismo hash
    nuevo = (hist
             + [(k, 0, activos[k]) for k in apagar]               # ACT_REG = 0
             + [(k, 1, activos[k]) for k in sin_cambio]
             + [(k, 1, 1) for k in insertar])                     # VRS_REG = 1
    return nuevo, dict(insertados=len(insertar), apagados=len(apagar), sin_cambio=len(sin_cambio))


def resumen(titulo, gold, m=None):
    act = sum(1 for r in gold if r[1] == 1)
    print(f"{titulo}: total={len(gold):,} activos={act:,} historicos={len(gold) - act:,}" + (f"  metricas={m}" if m else ""))
    return act


# Estado ANTES de la corrida del 2 de noviembre
gold = [(i, 1, 1) for i in range(1, N + 1)] + [(-i, 0, 1) for i in range(1, H + 1)]
resumen("Antes (5 oct)            ", gold)

# 2 de noviembre: extraccion incremental (EXCLUIR_EXTRAIDOS) -> la entrada son solo los K nuevos
nuevos = set(range(N + 1, N + K + 1))
gold, m = scd(gold, nuevos)
act_dano = resumen("Tras 2 nov (el fallo)     ", gold, m)
assert m["apagados"] == N and act_dano == K, "no reproduce el fallo"

# Propuesta A: limpiar los segmentos y volver a correr -> extraccion completa (<= periodo)
completa = set(range(1, N + K + 1))
gold_a, ma = scd(gold, completa)
act_a = resumen("A) Limpiar seg. + rerun  ", gold_a, ma)
dup = sum(1 for k, a, v in gold_a if k > 0) - len({k for k, a, v in gold_a if k > 0})
print(f"   llaves con DOS filas (una cerrada el 2 nov + una activa reiniciada en version 1): {dup:,}")
assert act_a == N + K, "los activos deberian recuperarse"

# Siguiente re-ejecucion del MISMO periodo despues de limpiar: las llaves se vuelven a guardar
# (segmento recreado) y aparecen M endosos nuevos -> vuelve a pasar lo mismo.
M = 150
gold_b, mb = scd(gold_a, set(range(N + K + 1, N + K + M + 1)))
act_b = resumen("B) Nuevo rerun incremental", gold_b, mb)
assert mb["apagados"] == N + K and act_b == M, "se repite el fallo"

print("\nConclusion de la simulacion:")
print(" - Limpiar los segmentos RECUPERA los activos (A), pero deja historico duplicado y reinicia versiones.")
print(" - NO corrige la causa: la siguiente re-ejecucion incremental del periodo repite el fallo (B).")
