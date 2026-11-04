"""Construcción de los cuatro JSON de la malla: extracción, validación, remediación y SCD.

Reglas de negocio puras: reciben la configuración y los puertos de lectura que
necesitan; no conocen boto3.
"""
from .errores import ErrorConfiguracion
from .validaciones import claves_ciclo

SK_ESTRUCTURA_ORIGEN = "SRC"
SK_ESTRUCTURA_GOLD = "GLD"
CAMPO_REGLAS_TRANSFORMATIVAS = "reglasTransformativas"


def construir_todos(cnf, prs, almacen, spec):
    return {
        "EXTRACCION": construir_extraccion(cnf, prs, spec),
        "VALIDACION": construir_validacion(cnf),
        "REMEDIACION": construir_remediacion(cnf, almacen, spec),
        "SCD": construir_scd(cnf, prs),
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
        "ciclos": [_ciclo(cnf[clave], prs, spec.formato_tabla)
                   for clave in claves_ciclo(spec.total_ciclos)],
        "salida_runtime": config["salidaRuntime"],
    }


def contar_estructuras_prs(cnf, spec):
    """Estructuras distintas validadas en PRS: las de origen de los ciclos más la Gold."""
    referencias = {
        uso["estructuraRef"]
        for clave in claves_ciclo(spec.total_ciclos)
        for segmento in cnf[clave]["segmentos"]
        for uso in segmento["estructuras"]
    }
    return len(referencias) + 1


def construir_validacion(cnf):
    config = cnf["VALIDACION"]
    return {
        "nombre_caso": cnf["GENERAL"]["nombreCaso"],
        "parametros_extraccion": config["parametrosExtraccion"],
        "salida": config["salida"],
        "datasets": config["datasets"],
    }


def construir_remediacion(cnf, almacen, spec):
    config = cnf["REMEDIACION"]
    estado = config.get("estadoDefinicion")
    esperado = estado_remediacion_esperado(config, spec.remediacion)
    if estado != esperado:
        raise ErrorConfiguracion("Estado de definición de remediación inesperado",
                                 codigo="CFG-CNF-007",
                                 detalle={"esperado": esperado, "actual": estado})
    plantilla = almacen.leer(config["plantillaUri"])
    if plantilla.get("nombre_caso") != spec.nombre_caso:
        raise ErrorConfiguracion("La plantilla de remediación no corresponde a la malla",
                                 codigo="CFG-CNF-007",
                                 detalle={"plantilla": plantilla.get("nombre_caso")})
    return plantilla


def estado_remediacion_esperado(config, reglas):
    if reglas.estado_con_reglas is not None and config.get(CAMPO_REGLAS_TRANSFORMATIVAS):
        return reglas.estado_con_reglas
    return reglas.estado_sin_reglas


def construir_scd(cnf, prs):
    config = cnf["SCD"]
    referencia = config["estructuraGoldRef"]
    if not prs.obtener(referencia, SK_ESTRUCTURA_GOLD):
        raise ErrorConfiguracion("Estructura Gold inexistente en PRS", codigo="CFG-CNF-005",
                                 detalle={"referencia": referencia})
    return {
        "nombre_caso": cnf["GENERAL"]["nombreCaso"],
        "salida": config["salida"],
        "llaves": config["llaves"],
        "columnas_cambio": config["columnasCambio"],
        "columnas_fijas": config["columnasFijas"],
        "columnas_administrativas": config["columnasAdministrativas"],
        "metadatos": config["metadatos"],
    }


def _ciclo(item, prs, formato):
    salida = {
        "orden": item["orden"],
        "nombre": item["nombre"],
        "segmentos": [_segmento(s, prs, formato) for s in sorted(item["segmentos"],
                                                                key=lambda s: s["orden"])],
    }
    if item.get("descripcion") is not None:
        salida["descripcion"] = item["descripcion"]
    return salida


def _segmento(segmento, prs, formato):
    salida = {
        "orden": segmento["orden"],
        "nombre": segmento["nombre"],
        "forma": segmento["forma"],
        "tablas": [_tabla(uso, prs, formato) for uso in sorted(segmento["estructuras"],
                                                              key=lambda u: u["orden"])],
    }
    if segmento.get("descripcion") is not None:
        salida["descripcion"] = segmento["descripcion"]
    return salida


def _tabla(uso, prs, formato):
    referencia = uso["estructuraRef"]
    estructura = prs.obtener(referencia, SK_ESTRUCTURA_ORIGEN)
    if not estructura:
        raise ErrorConfiguracion("Estructura inexistente en PRS", codigo="CFG-CNF-005",
                                 detalle={"referencia": referencia})
    schema, tabla = estructura["schema"], estructura["tabla"]
    salida = {
        "orden": uso["orden"],
        "schema": schema,
        "tabla": tabla,
        "tabla_relativa": f"{schema.lower()}/{tabla.lower()}",
        "tipo_tabla": uso["tipoTabla"],
    }
    for campo_cnf, campo_json in formato.campos_opcionales:
        valor = uso.get(campo_cnf)
        if valor is not None or not formato.omitir_nulos:
            salida[campo_json] = valor
    salida["segmentacion_horizontal"] = uso["segmentacionHorizontal"]
    return salida
