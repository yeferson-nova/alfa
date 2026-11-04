"""Construcción de los cuatro JSON de la malla: extracción, validación, remediación y SCD.

Reglas de negocio puras: reciben la configuración y los puertos de lectura que
necesitan; no conocen boto3.
"""
from .errores import ErrorConfiguracion
from .validaciones import claves_ciclo

SK_ESTRUCTURA_ORIGEN = "SRC"
SK_ESTRUCTURA_GOLD = "GLD"


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
        "ciclos": [_ciclo(cnf[clave], prs) for clave in claves_ciclo(spec.total_ciclos)],
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


def construir_remediacion(cnf, almacen, spec):
    config = cnf["REMEDIACION"]
    estado = config.get("estadoDefinicion")
    if estado not in spec.estados_remediacion:
        raise ErrorConfiguracion("Estado de definición de remediación no permitido",
                                 codigo="CFG-CNF-007",
                                 detalle={"estado": estado,
                                          "permitidos": list(spec.estados_remediacion)})
    plantilla = almacen.leer(config["plantillaUri"])
    if plantilla.get("nombre_caso") != spec.nombre_caso:
        raise ErrorConfiguracion("La plantilla de remediación no corresponde a la malla",
                                 codigo="CFG-CNF-007",
                                 detalle={"plantilla": plantilla.get("nombre_caso")})
    return plantilla


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


def _ciclo(item, prs):
    salida = {
        "orden": item["orden"],
        "nombre": item["nombre"],
        "segmentos": [_segmento(s, prs) for s in sorted(item["segmentos"],
                                                       key=lambda s: s["orden"])],
    }
    if item.get("descripcion") is not None:
        salida["descripcion"] = item["descripcion"]
    return salida


def _segmento(segmento, prs):
    salida = {
        "orden": segmento["orden"],
        "nombre": segmento["nombre"],
        "forma": segmento["forma"],
        "tablas": [_tabla(uso, prs) for uso in sorted(segmento["estructuras"],
                                                     key=lambda u: u["orden"])],
    }
    if segmento.get("descripcion") is not None:
        salida["descripcion"] = segmento["descripcion"]
    return salida


def _tabla(uso, prs):
    referencia = uso["estructuraRef"]
    estructura = prs.obtener(referencia, SK_ESTRUCTURA_ORIGEN)
    if not estructura:
        raise ErrorConfiguracion("Estructura inexistente en PRS", codigo="CFG-CNF-005",
                                 detalle={"referencia": referencia})
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
