import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlparse

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ["TABLE_NAME"]
FUNCTION_NAME = os.environ["FUNCTION_NAME"]
INICIADOR_FUNCTION_NAME = os.environ["INICIADOR_FUNCTION_NAME"]

table = boto3.resource("dynamodb").Table(TABLE_NAME)
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")


def convertir_decimal(valor):
    if isinstance(valor, Decimal):
        return int(valor) if valor % 1 == 0 else float(valor)
    if isinstance(valor, list):
        return [convertir_decimal(x) for x in valor]
    if isinstance(valor, dict):
        return {
            clave: convertir_decimal(contenido)
            for clave, contenido in valor.items()
        }
    return valor


def separar_s3(uri):
    parsed = urlparse(uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"URI S3 invalida: {uri}")

    return parsed.netloc, parsed.path.lstrip("/")


def leer_json_s3(uri):
    bucket, key = separar_s3(uri)
    respuesta = s3.get_object(Bucket=bucket, Key=key)

    return json.loads(
        respuesta["Body"].read().decode("utf-8")
    )


def escribir_json_s3(uri, contenido):
    bucket, key = separar_s3(uri)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            contenido,
            ensure_ascii=False,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "generado-por": FUNCTION_NAME,
            "fecha-generacion": (
                datetime.now(timezone.utc).isoformat()
            )
        }
    )


def consultar_malla(malla):
    pk = f"CNF#MALLA#{malla.upper()}"

    respuesta = table.query(
        KeyConditionExpression=Key("PK").eq(pk),
        ConsistentRead=True
    )

    items = [
        convertir_decimal(item)
        for item in respuesta.get("Items", [])
    ]

    while "LastEvaluatedKey" in respuesta:
        respuesta = table.query(
            KeyConditionExpression=Key("PK").eq(pk),
            ExclusiveStartKey=respuesta["LastEvaluatedKey"],
            ConsistentRead=True
        )

        items.extend(
            convertir_decimal(item)
            for item in respuesta.get("Items", [])
        )

    if not items:
        raise ValueError(
            f"No existe configuracion para la malla {malla}."
        )

    return items


def obtener_unico(items, tipo):
    encontrados = [
        item
        for item in items
        if item.get("tipoRegistro") == tipo
    ]

    if len(encontrados) != 1:
        raise ValueError(
            f"Se esperaba un registro {tipo}, "
            f"pero se encontraron {len(encontrados)}."
        )

    return encontrados[0]


def actualizar_tabla_dinamica(tabla, periodo):
    tabla = convertir_decimal(tabla)

    segmentacion = tabla.get("segmentacion_horizontal")

    if isinstance(segmentacion, dict):
        if segmentacion.get("habilitada") is True:
            segmentacion["periodo_negocio"] = periodo

            segmento_id = segmentacion.get("segmento_id")

            if segmento_id:
                segmentacion["segmento_id"] = re.sub(
                    r"(?<=niif17_)\d{6}",
                    periodo,
                    segmento_id
                )

    condicion = tabla.get("where")

    if condicion and "PERIODO" in condicion.upper():
        tabla["where"] = re.sub(
            r"'\d{6}'",
            f"'{periodo}'",
            condicion
        )

    return tabla


def reconstruir_ciclos(items, periodo):
    ciclos = sorted(
        [
            item
            for item in items
            if item.get("tipoRegistro") == "CNF_CICLO"
        ],
        key=lambda x: x["orden"]
    )

    conjuntos = [
        item
        for item in items
        if item.get("tipoRegistro") == "CNF_CONJUNTO"
    ]

    asociaciones = [
        item
        for item in items
        if item.get("tipoRegistro")
        == "CNF_CONJUNTO_ESTRUCTURA"
    ]

    resultado = []

    for ciclo in ciclos:
        ciclo_orden = ciclo["orden"]

        conjuntos_ciclo = sorted(
            [
                conjunto
                for conjunto in conjuntos
                if conjunto["cicloOrden"] == ciclo_orden
            ],
            key=lambda x: x["orden"]
        )

        segmentos = []

        for conjunto in conjuntos_ciclo:
            conjunto_orden = conjunto["orden"]

            estructuras = sorted(
                [
                    asociacion
                    for asociacion in asociaciones
                    if (
                        asociacion["cicloOrden"] == ciclo_orden
                        and asociacion["conjuntoOrden"]
                        == conjunto_orden
                    )
                ],
                key=lambda x: x["ordenEjecucion"]
            )

            tablas = []

            for estructura in estructuras:
                tabla = actualizar_tabla_dinamica(
                    estructura["configuracionTabla"],
                    periodo
                )

                tablas.append(tabla)

            segmentos.append({
                "orden": conjunto_orden,
                "nombre": conjunto["nombre"],
                "forma": conjunto["forma"],
                "tablas": tablas
            })

        ciclo_salida = {
            "orden": ciclo_orden,
            "nombre": ciclo["nombre"],
            "segmentos": segmentos
        }

        if ciclo.get("descripcion") is not None:
            ciclo_salida["descripcion"] = ciclo["descripcion"]

        resultado.append(ciclo_salida)

    if not resultado:
        raise ValueError("La malla no contiene ciclos.")

    return resultado


def resolver_modo(tipo_evento):
    modos = {
        "CARGUE": "INICIAL",
        "REPROCESO": "REPROCESO",
        "RECARGUE_HISTORICO": "RECARGUE_HISTORICO"
    }

    return modos.get(tipo_evento, "INICIAL")


def validar_configuracion(config):
    requeridos = [
        "nombre_caso",
        "periodo_negocio",
        "fuente",
        "rutas",
        "control_extraccion",
        "ciclos",
        "salida_runtime"
    ]

    faltantes = [
        campo
        for campo in requeridos
        if campo not in config
    ]

    if faltantes:
        raise ValueError(
            "Campos faltantes: " + ", ".join(faltantes)
        )

    for ciclo in config["ciclos"]:
        if not ciclo.get("segmentos"):
            raise ValueError(
                f"Ciclo sin segmentos: {ciclo.get('nombre')}"
            )

        for segmento in ciclo["segmentos"]:
            if not segmento.get("tablas"):
                raise ValueError(
                    f"Conjunto sin estructuras: "
                    f"{segmento.get('nombre')}"
                )



def registrar_extraccion(
    malla,
    periodo,
    tipo_evento,
    modo_ejecucion,
    id_programacion,
    id_automatizacion,
    ruta_config,
    ruta_config_validacion,
    ruta_parametros
):
    id_extraccion = f"EXT#{id_automatizacion}"

    item = {
        "PK": f"EJC#AUTOMATIZACION#{id_automatizacion}",
        "SK": "EXTRACCION",
        "tipoRegistro": "EJC_EXTRACCION",
        "idExtraccion": id_extraccion,
        "idAutomatizacion": id_automatizacion,
        "idProgramacion": id_programacion,
        "malla": malla,
        "periodoNegocio": periodo,
        "tipoEvento": tipo_evento,
        "modoEjecucion": modo_ejecucion,
        "estado": "CONFIGURACION_GENERADA",
        "rutaConfigExtraccion": ruta_config,
        "rutaConfigValidacion": ruta_config_validacion,
        "rutaParametrosExtraccion": ruta_parametros,
        "fechaMaterializacion": (
            datetime.now(timezone.utc).isoformat()
        ),
        "activo": True
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(PK) "
                "AND attribute_not_exists(SK)"
            )
        )

        logger.info(
            "Extraccion registrada: %s",
            id_extraccion
        )

        return item, False

    except ClientError as error:
        codigo = error.response.get("Error", {}).get("Code")

        if codigo != "ConditionalCheckFailedException":
            raise

        respuesta = table.get_item(
            Key={
                "PK": item["PK"],
                "SK": item["SK"]
            },
            ConsistentRead=True
        )

        existente = respuesta.get("Item")

        if not existente:
            raise ValueError(
                "La extraccion existe, pero no pudo recuperarse."
            )

        if (
            existente.get("rutaConfigExtraccion")
            != ruta_config
        ):
            raise ValueError(
                "La automatizacion ya tiene otra extraccion."
            )

        logger.info(
            "Extraccion existente validada: %s",
            existente.get("idExtraccion")
        )

        return convertir_decimal(existente), True



def invocar_iniciador(id_automatizacion):
    payload = {
        "idAutomatizacion": id_automatizacion
    }

    respuesta = lambda_client.invoke(
        FunctionName=INICIADOR_FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(
            payload,
            ensure_ascii=False
        ).encode("utf-8")
    )

    status_code = respuesta.get("StatusCode")

    if status_code != 202:
        raise RuntimeError(
            "Invocacion asincrona al Iniciador "
            f"no aceptada. StatusCode={status_code}"
        )

    request_id = respuesta.get(
        "ResponseMetadata",
        {}
    ).get("RequestId")

    logger.info(
        "Iniciador invocado para %s. RequestId=%s",
        id_automatizacion,
        request_id
    )

    return request_id



def resolver_configuracion_tecnica(malla, version=1):
    codigo_malla = str(malla).strip().upper()
    codigo_malla = re.sub(
        r"[^A-Z0-9]+",
        "_",
        codigo_malla
    ).strip("_")

    respuesta = table.get_item(
        Key={
            "PK": f"CNF#MALLA#{codigo_malla}",
            "SK": f"VERSION#{version:03d}"
        },
        ConsistentRead=True
    )

    catalogo = respuesta.get("Item")

    if catalogo:
        if catalogo.get("estado") != "ACTIVA":
            raise ValueError(
                f"Catalogo tecnico inactivo para {codigo_malla}"
            )

        configuracion = {
            "templateExtraccionUri":
                catalogo.get("templateExtraccionUri"),
            "templateValidacionUri":
                catalogo.get("templateValidacionUri"),
            "outputBase":
                catalogo.get("outputBase"),
            "parametersBase":
                catalogo.get("parametersBase"),
            "modoEjecucionDefecto":
                catalogo.get(
                    "modoEjecucionDefecto",
                    "INICIAL"
                )
        }

        faltantes = [
            campo
            for campo, valor in configuracion.items()
            if not valor
        ]

        if faltantes:
            raise ValueError(
                "Catalogo tecnico incompleto para "
                f"{codigo_malla}: {faltantes}"
            )

        logger.info(
            "Configuracion tecnica resuelta desde catalogo. "
            "Malla=%s Version=%03d",
            codigo_malla,
            version
        )

        return configuracion, "CATALOGO"

    raise ValueError(
        "No existe configuracion tecnica activa "
        f"para {codigo_malla} VERSION#{version:03d}"
    )


def lambda_handler(event, context):
    logger.info(
        "Entrada: %s",
        json.dumps(event, ensure_ascii=False)
    )

    malla = str(event["malla"]).strip().lower()
    periodo = str(event["periodoNegocio"])
    tipo_evento = str(
        event.get("tipoEvento", "CARGUE")
    ).upper()

    id_programacion = event["idProgramacion"]
    id_automatizacion = event["idAutomatizacion"]

    configuracion_tecnica, origen_tecnico = (
        resolver_configuracion_tecnica(malla)
    )

    template_uri = configuracion_tecnica[
        "templateExtraccionUri"
    ]
    validation_template_uri = configuracion_tecnica[
        "templateValidacionUri"
    ]
    output_base = configuracion_tecnica["outputBase"]
    parameters_base = configuracion_tecnica[
        "parametersBase"
    ]
    modo_ejecucion_defecto = configuracion_tecnica[
        "modoEjecucionDefecto"
    ]

    if not re.fullmatch(r"\d{6}", periodo):
        raise ValueError(
            "periodoNegocio debe tener formato YYYYMM."
        )

    items = consultar_malla(malla)
    metadata = obtener_unico(items, "CNF_MALLA")

    if metadata.get("estado") != "ACTIVA":
        raise ValueError("La malla no esta ACTIVA.")

    plantilla = leer_json_s3(template_uri)

    ruta_config = (
        f"{output_base.rstrip('/')}/{malla}/"
        f"periodo={periodo}/{id_automatizacion}.json"
    )

    ruta_parametros = (
        f"{parameters_base.rstrip('/')}/{malla}/"
        f"periodo={periodo}/{id_automatizacion}/"
        "parametros_extraccion.json"
    )

    ruta_config_validacion = (
        f"{output_base.rstrip('/')}/{malla}/"
        f"periodo={periodo}/"
        f"{id_automatizacion}_validacion.json"
    )

    config = plantilla

    config["nombre_caso"] = metadata["nombre"]
    config["periodo_negocio"] = periodo
    config["fuente"] = metadata["fuente"]
    config["rutas"] = metadata["rutas"]
    config["control_extraccion"] = (
        metadata["controlExtraccion"]
    )
    config["salida_runtime"] = metadata["salidaRuntime"]

    config["control_extraccion"][
        "periodo_negocio"
    ] = periodo

    config["control_extraccion"][
        "modo_ejecucion"
    ] = resolver_modo(tipo_evento)

    config["rutas"][
        "ruta_parametros_extraccion"
    ] = ruta_parametros

    config["ciclos"] = reconstruir_ciclos(
        items,
        periodo
    )

    validar_configuracion(config)
    escribir_json_s3(ruta_config, config)

    config_validacion = leer_json_s3(
        validation_template_uri
    )

    config_validacion.setdefault(
        "parametros_extraccion",
        {}
    )["ruta"] = ruta_parametros

    escribir_json_s3(
        ruta_config_validacion,
        config_validacion
    )

    extraccion, extraccion_existente = registrar_extraccion(
        malla=malla,
        periodo=periodo,
        tipo_evento=tipo_evento,
        modo_ejecucion=(
            config["control_extraccion"]["modo_ejecucion"]
        ),
        id_programacion=id_programacion,
        id_automatizacion=id_automatizacion,
        ruta_config=ruta_config,
        ruta_config_validacion=ruta_config_validacion,
        ruta_parametros=ruta_parametros
    )

    total_segmentos = sum(
        len(ciclo["segmentos"])
        for ciclo in config["ciclos"]
    )

    total_tablas = sum(
        len(segmento["tablas"])
        for ciclo in config["ciclos"]
        for segmento in ciclo["segmentos"]
    )

    iniciador_request_id = invocar_iniciador(
        id_automatizacion
    )

    resultado = {
        "estado": "CONFIGURACION_GENERADA",
        "origenConfiguracion": "DYNAMODB_NORMALIZADO",
        "origenConfiguracionTecnica": origen_tecnico,
        "versionConfiguracionTecnica": 1,
        "templateExtraccionUri": template_uri,
        "templateValidacionUri": validation_template_uri,
        "outputBase": output_base,
        "parametersBase": parameters_base,
        "modoEjecucionDefecto": modo_ejecucion_defecto,
        "malla": malla,
        "periodoNegocio": periodo,
        "modoEjecucion": (
            config["control_extraccion"]["modo_ejecucion"]
        ),
        "idProgramacion": id_programacion,
        "idAutomatizacion": id_automatizacion,
        "idExtraccion": extraccion["idExtraccion"],
        "estadoExtraccion": extraccion["estado"],
        "extraccionExistente": extraccion_existente,
        "iniciadorInvocado": True,
        "iniciadorRequestId": iniciador_request_id,
        "rutaConfigExtraccion": ruta_config,
        "rutaConfigValidacion": ruta_config_validacion,
        "rutaParametrosExtraccion": ruta_parametros,
        "totalCiclos": len(config["ciclos"]),
        "totalSegmentos": total_segmentos,
        "totalTablas": total_tablas
    }

    logger.info(
        "Resultado: %s",
        json.dumps(resultado, ensure_ascii=False)
    )

    return resultado
