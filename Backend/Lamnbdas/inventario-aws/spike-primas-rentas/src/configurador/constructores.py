"""Construcción de los cuatro JSON (extracción, validación, remediación, SCD).

Reglas de negocio: reciben la configuración y los puertos de lectura que
necesitan; no conocen boto3.
"""
from .errores import ConfiguracionError


def construir_ciclos(cnf, prs, spec):
    claves = sorted(k for k in cnf if k.startswith("CICLO#"))
    if len(claves) != spec.total_ciclos:
        raise ConfiguracionError(
            f"Ciclos esperados={spec.total_ciclos}, encontrados={len(claves)}"
        )
    return [_ciclo(cnf[clave], prs) for clave in claves]


def _ciclo(item, prs):
    salida = {
        "orden": item["orden"],
        "nombre": item["nombre"],
        "segmentos": [
            _segmento(s, prs) for s in sorted(item["segmentos"], key=lambda s: s["orden"])
        ],
    }
    if item.get("descripcion") is not None:
        salida["descripcion"] = item["descripcion"]
    return salida


def _segmento(segmento, prs):
    salida = {
        "orden": segmento["orden"],
        "nombre": segmento["nombre"],
        "forma": segmento["forma"],
        "tablas": [
            _tabla(uso, prs)
            for uso in sorted(segmento["estructuras"], key=lambda u: u["orden"])
        ],
    }
    if segmento.get("descripcion") is not None:
        salida["descripcion"] = segmento["descripcion"]
    return salida


def _tabla(uso, prs):
    referencia = uso["estructuraRef"]
    estructura = prs.obtener(referencia, "SRC")
    if not estructura:
        raise ConfiguracionError(f"Estructura inexistente en PRS: {referencia}")
    schema, tabla = estructura["schema"], estructura["tabla"]
    return {
        "orden": uso["orden"],
        "schema": schema,
        "tabla": tabla,
        "tabla_relativa": f"{schema.lower()}/{tabla.lower()}",
        "tipo_tabla": uso["tipoTabla"],
        "where": uso.get("where"),
        "dependencia": uso.get("dependencia"),
        "segmentacion_horizontal": uso["segmentacionHorizontal"],
    }


def construir_extraccion(cnf, prs, spec):
    config = cnf["EXTRACCION"]
    control = config["controlExtraccion"]
    return {
        "nombre_caso": cnf["GENERAL"]["nombreCaso"],
        "periodo_negocio": control["periodo_negocio"],
        "fuente": config["fuente"],
        "rutas": config["rutas"],
        "control_extraccion": control,
        "ciclos": construir_ciclos(cnf, prs, spec),
        "salida_runtime": config["salidaRuntime"],
    }


def construir_validacion(cnf):
    config = cnf["VALIDACION"]
    return {
        "nombre_caso": cnf["GENERAL"]["nombreCaso"],
        "parametros_extraccion": config["parametrosExtraccion"],
        "salida": config["salida"],
        "datasets": config["datasets"],
    }


def construir_scd(cnf, prs):
    config = cnf["SCD"]
    gold_ref = config["estructuraGoldRef"]
    if not prs.obtener(gold_ref, "GLD"):
        raise ConfiguracionError(f"Estructura Gold inexistente: {gold_ref}")
    return {
        "nombre_caso": cnf["GENERAL"]["nombreCaso"],
        "salida": config["salida"],
        "llaves": config["llaves"],
        "columnas_cambio": config["columnasCambio"],
        "columnas_fijas": config["columnasFijas"],
        "columnas_administrativas": config["columnasAdministrativas"],
        "metadatos": config["metadatos"],
    }


def construir_remediacion(cnf, almacen, spec):
    config = cnf["REMEDIACION"]
    plantilla = almacen.leer(config["plantillaUri"])
    if plantilla.get("nombre_caso") != spec.nombre_caso:
        raise ConfiguracionError(
            f"Plantilla de remediacion no corresponde a {spec.nombre}"
        )
    if config.get("estadoDefinicion") != spec.estado_remediacion:
        raise ConfiguracionError("Estado de remediacion inesperado")
    return plantilla
