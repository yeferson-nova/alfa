import json
import logging
import os
from decimal import Decimal
from urllib.parse import urlparse

import boto3
from boto3.dynamodb.conditions import Key


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

REGION = os.environ["AWS_REGION"]
PRS_TABLE = os.environ["PRS_TABLE"]
CNF_TABLE = os.environ["CNF_TABLE"]
MALLA_PK = os.environ.get(
    "MALLA_PK",
    "MALLA#RESERVA_MATEMATICA_ARP"
)

DYNAMODB = boto3.resource(
    "dynamodb",
    region_name=REGION
)
S3 = boto3.client(
    "s3",
    region_name=REGION
)

PRS = DYNAMODB.Table(PRS_TABLE)
CNF = DYNAMODB.Table(CNF_TABLE)


class ConfiguracionError(Exception):
    pass


def decimal_default(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)

    raise TypeError(
        f"Tipo no serializable: {type(value)}"
    )


def normalizar(value):
    return json.loads(
        json.dumps(
            value,
            default=decimal_default
        )
    )


def leer_json_s3(uri):
    parsed = urlparse(uri)

    if parsed.scheme != "s3":
        raise ConfiguracionError(
            f"URI S3 invalida: {uri}"
        )

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    response = S3.get_object(
        Bucket=bucket,
        Key=key
    )

    contenido = response["Body"].read()

    return json.loads(
        contenido.decode("utf-8")
    )


def escribir_json_s3(uri, contenido):
    parsed = urlparse(uri)

    if parsed.scheme != "s3":
        raise ConfiguracionError(
            f"URI S3 invalida: {uri}"
        )

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    cuerpo = json.dumps(
        contenido,
        ensure_ascii=False,
        indent=2,
        default=decimal_default
    ) + "\n"

    response = S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=cuerpo.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "origen": "dynamodb-prs-cnf",
            "malla": "reserva-matematica-arp"
        }
    )

    return {
        "uri": uri,
        "etag": response.get(
            "ETag",
            ""
        ).replace('"', ""),
        "versionId": response.get(
            "VersionId"
        )
    }


def consultar_cnf():
    response = CNF.query(
        KeyConditionExpression=Key("PK").eq(
            MALLA_PK
        ),
        ConsistentRead=True
    )

    items = response.get("Items", [])

    if len(items) != 8:
        raise ConfiguracionError(
            f"CNF esperada=8, encontrada={len(items)}"
        )

    return {
        item["SK"]: normalizar(item)
        for item in items
    }


def obtener_estructura_prs(referencia):
    response = PRS.get_item(
        Key={
            "PK": referencia,
            "SK": "SRC"
        },
        ConsistentRead=True
    )

    item = response.get("Item")

    if not item:
        raise ConfiguracionError(
            f"Estructura inexistente en PRS: "
            f"{referencia}"
        )

    return normalizar(item)


def construir_ciclos(cnf):
    ciclos = []

    claves_ciclo = sorted(
        clave
        for clave in cnf
        if clave.startswith("CICLO#")
    )

    if len(claves_ciclo) != 3:
        raise ConfiguracionError(
            f"Ciclos esperados=3, "
            f"encontrados={len(claves_ciclo)}"
        )

    for clave_ciclo in claves_ciclo:
        item_ciclo = cnf[clave_ciclo]
        segmentos_salida = []

        for segmento in sorted(
            item_ciclo["segmentos"],
            key=lambda item: item["orden"]
        ):
            tablas_salida = []

            for uso in sorted(
                segmento["estructuras"],
                key=lambda item: item["orden"]
            ):
                referencia = uso["estructuraRef"]
                estructura = obtener_estructura_prs(
                    referencia
                )

                schema = estructura["schema"]
                tabla = estructura["tabla"]

                tabla_salida = {
                    "orden": uso["orden"],
                    "schema": schema,
                    "tabla": tabla,
                    "tabla_relativa": (
                        f"{schema.lower()}/"
                        f"{tabla.lower()}"
                    ),
                    "tipo_tabla": uso["tipoTabla"],
                    "segmentacion_horizontal": (
                        uso["segmentacionHorizontal"]
                    )
                }

                if uso.get("where") is not None:
                    tabla_salida["where"] = uso["where"]

                if uso.get("dependencia") is not None:
                    tabla_salida["dependencia"] = (
                        uso["dependencia"]
                    )

                if uso.get("reutilizacionCompartida") is not None:
                    tabla_salida["reutilizacion_compartida"] = (
                        uso["reutilizacionCompartida"]
                    )

                tablas_salida.append(
                    tabla_salida
                )

            segmento_salida = {
                "orden": segmento["orden"],
                "nombre": segmento["nombre"],
                "forma": segmento["forma"],
                "tablas": tablas_salida
            }

            if segmento.get("descripcion") is not None:
                segmento_salida["descripcion"] = (
                    segmento["descripcion"]
                )

            segmentos_salida.append(
                segmento_salida
            )

        ciclo_salida = {
            "orden": item_ciclo["orden"],
            "nombre": item_ciclo["nombre"],
            "segmentos": segmentos_salida
        }

        if item_ciclo.get("descripcion") is not None:
            ciclo_salida["descripcion"] = (
                item_ciclo["descripcion"]
            )

        ciclos.append(ciclo_salida)

    return ciclos


def construir_extraccion(cnf):
    general = cnf["GENERAL"]
    config = cnf["EXTRACCION"]

    periodo = config["controlExtraccion"][
        "periodo_negocio"
    ]

    return {
        "nombre_caso": general["nombreCaso"],
        "periodo_negocio": periodo,
        "fuente": config["fuente"],
        "rutas": config["rutas"],
        "control_extraccion": (
            config["controlExtraccion"]
        ),
        "ciclos": construir_ciclos(cnf),
        "salida_runtime": config["salidaRuntime"]
    }


def construir_validacion(cnf):
    general = cnf["GENERAL"]
    config = cnf["VALIDACION"]

    return {
        "nombre_caso": general["nombreCaso"],
        "parametros_extraccion": (
            config["parametrosExtraccion"]
        ),
        "salida": config["salida"],
        "datasets": config["datasets"]
    }


def construir_scd(cnf):
    general = cnf["GENERAL"]
    config = cnf["SCD"]

    gold_ref = config["estructuraGoldRef"]

    response = PRS.get_item(
        Key={
            "PK": gold_ref,
            "SK": "GLD"
        },
        ConsistentRead=True
    )

    if "Item" not in response:
        raise ConfiguracionError(
            f"Estructura Gold inexistente: {gold_ref}"
        )

    return {
        "nombre_caso": general["nombreCaso"],
        "salida": config["salida"],
        "llaves": config["llaves"],
        "columnas_cambio": (
            config["columnasCambio"]
        ),
        "columnas_fijas": (
            config["columnasFijas"]
        ),
        "columnas_administrativas": (
            config["columnasAdministrativas"]
        ),
        "metadatos": config["metadatos"]
    }


def construir_remediacion(cnf):
    config = cnf["REMEDIACION"]

    plantilla = leer_json_s3(
        config["plantillaUri"]
    )

    if plantilla.get(
        "nombre_caso"
    ) != "reserva_matematica_arp":
        raise ConfiguracionError(
            "Plantilla de remediacion no corresponde "
            "a Reserva Matematica ARP"
        )

    reglas = config.get(
        "reglasTransformativas",
        []
    )

    estado_esperado = (
        "DEFINIDA_CON_REGLAS_TRANSFORMATIVAS"
        if reglas
        else "PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS"
    )

    if config.get("estadoDefinicion") != estado_esperado:
        raise ConfiguracionError(
            "Estado de remediacion inesperado: "
            f"esperado={estado_esperado}, "
            f"actual={config.get('estadoDefinicion')}"
        )

    return plantilla


def comparar(nombre, generado, plantilla):
    generado_normalizado = normalizar(generado)
    plantilla_normalizada = normalizar(plantilla)

    return {
        "nombre": nombre,
        "coincide": (
            generado_normalizado
            == plantilla_normalizada
        )
    }


def aplicar_periodo_runtime(contenido, periodo_anterior, periodo_nuevo):
    from datetime import date, timedelta

    periodo = str(periodo_nuevo)
    if len(periodo) != 6 or not periodo.isdigit():
        raise ValueError("periodoNegocio debe tener formato YYYYMM")

    anio = int(periodo[:4])
    mes = int(periodo[4:])
    inicio = date(anio, mes, 1)
    if mes == 12:
        siguiente = date(anio + 1, 1, 1)
    else:
        siguiente = date(anio, mes + 1, 1)

    cambios = {
        "${PERIODO_NEGOCIO}": periodo,
        "${FECHA_INICIO}": inicio.isoformat(),
        "${FECHA_SIGUIENTE_MES}": siguiente.isoformat(),
        "${FECHA_FIN}": (siguiente - timedelta(days=1)).isoformat(),
    }

    def resolver(valor):
        if isinstance(valor, dict):
            return {k: resolver(v) for k, v in valor.items()}
        if isinstance(valor, list):
            return [resolver(v) for v in valor]
        if isinstance(valor, str):
            resultado = valor.replace(str(periodo_anterior), periodo)
            for marcador, reemplazo in cambios.items():
                resultado = resultado.replace(marcador, reemplazo)
            return resultado
        return valor

    return resolver(contenido)


def lambda_handler(event, context):
    event = _evento_desde_prg(event or {})
    publicar = bool(event.get("publicar", False))

    ejecutar = bool(
        event.get(
            "ejecutar",
            publicar
        )
    )

    if ejecutar and not publicar:
        raise ConfiguracionError(
            "Para ejecutar la Step se requiere publicar=true"
        )

    LOGGER.info(
        "Inicio Configurador. publicar=%s ejecutar=%s",
        publicar,
        ejecutar
    )

    cnf = consultar_cnf()
    cnf, cnf_antes = _preparar_periodo_cnf(cnf, event)

    requeridos = {
        "GENERAL",
        "EXTRACCION",
        "VALIDACION",
        "REMEDIACION",
        "SCD",
        "CICLO#001",
        "CICLO#002",
        "CICLO#003"
    }

    faltantes = sorted(
        requeridos - set(cnf)
    )

    if faltantes:
        raise ConfiguracionError(
            f"Registros CNF faltantes: {faltantes}"
        )

    generados = {
        "EXTRACCION": construir_extraccion(cnf),
        "VALIDACION": construir_validacion(cnf),
        "REMEDIACION": construir_remediacion(cnf),
        "SCD": construir_scd(cnf)
    }

    periodo_runtime = event.get(
        "periodoNegocio"
    )
    periodo_base = None

    if periodo_runtime is not None:
        if (
            not isinstance(periodo_runtime, str)
            or len(periodo_runtime) != 6
            or not periodo_runtime.isdigit()
        ):
            raise ConfiguracionError(
                "periodoNegocio debe tener formato YYYYMM"
            )

        periodo_base = str(
            generados["EXTRACCION"][
                "periodo_negocio"
            ]
        )

        generados["EXTRACCION"] = (
            aplicar_periodo_runtime(
                generados["EXTRACCION"],
                periodo_base,
                periodo_runtime
            )
        )


    comparaciones = []

    _validar_generados_cnf(generados)



    publicados = []

    if publicar:
        _persistir_periodo_cnf(cnf, cnf_antes)
        for nombre, contenido in generados.items():
            uri_activa = cnf[nombre][
                "archivoActivoUri"
            ]

            publicados.append(
                escribir_json_s3(
                    uri_activa,
                    contenido
                )
            )

    periodo_resultado = (
        periodo_runtime
        if periodo_runtime is not None
        else generados["EXTRACCION"][
            "periodo_negocio"
        ]
    )

    execution_arn = None
    execution_name = None
    ejc_pk = None
    ejc_sk = None

    if ejecutar:
        from datetime import datetime, timezone
        import boto3

        fecha_inicio = datetime.now(
            timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        marca = datetime.now(
            timezone.utc
        ).strftime("%Y%m%dT%H%M%SZ")

        execution_name = (
            f"arp-{periodo_resultado}-"
            f"auto-{marca}-"
            f"{context.aws_request_id[:8]}"
        )

        ejc_pk = (
            "EJC#RESERVA_MATEMATICA_ARP#"
            f"{periodo_resultado}"
        )
        ejc_sk = (
            f"EJECUCION#{execution_name}"
        )

        step_input = {
            "ejcPK": ejc_pk,
            "ejcSK": ejc_sk,
            "malla": "RESERVA_MATEMATICA_ARP",
            "periodoNegocio": periodo_resultado,
            "modoEjecucion": event.get(
                "modoEjecucion",
                "NORMAL"
            ),
            "origen": event.get(
                "origen",
                "CONFIGURADOR"
            )
        }

        states = boto3.client(
            "stepfunctions"
        )

        ejecucion = states.start_execution(
            stateMachineArn=os.environ["STATE_MACHINE_ARN"],
            name=execution_name,
            input=json.dumps(step_input)
        )

        execution_arn = ejecucion[
            "executionArn"
        ]

        dynamodb = boto3.client(
            "dynamodb"
        )

        dynamodb.put_item(
            TableName=os.environ["EJC_TABLE"],
            Item={
                "PK": {"S": ejc_pk},
                "SK": {"S": ejc_sk},
                "malla": {
                    "S": "RESERVA_MATEMATICA_ARP"
                },
                "periodoNegocio": {
                    "S": periodo_resultado
                },
                "executionName": {
                    "S": execution_name
                },
                "executionArn": {
                    "S": execution_arn
                },
                "estado": {
                    "S": "INICIADA"
                },
                "fechaInicio": {
                    "S": fecha_inicio
                },
                "origen": {
                    "S": event.get(
                        "origen",
                        "CONFIGURADOR"
                    )
                }
            }
        )

    resultado = {
        "estado": "VALIDADO",
        "malla": MALLA_PK,
        "publicar": publicar,
        "ejecutar": ejecutar,
        "periodoNegocio": periodo_resultado,
        "registrosCNF": len(cnf),
        "estructurasPRSValidadas": 18,
        "comparaciones": comparaciones,
        "publicados": publicados,
        "executionName": execution_name,
        "executionArn": execution_arn,
        "ejcPK": ejc_pk,
        "ejcSK": ejc_sk
    }

    LOGGER.info(
        "Resultado: %s",
        json.dumps(resultado)
    )

    return resultado



def _resolver_where_periodo(where, viejo, nuevo):
    import re
    from datetime import date, timedelta

    inicio = date(int(nuevo[:4]), int(nuevo[4:]), 1)
    siguiente = date(
        inicio.year + (inicio.month == 12),
        1 if inicio.month == 12 else inicio.month + 1,
        1
    )
    fin = siguiente - timedelta(days=1)

    # Solo columnas de periodo identificadas en los filtros revisados.
    patron = (
        r"(\b(?:[A-Za-z_][A-Za-z_0-9]*\.)?"
        r"(?:FEC_ENVIO_TES|FEC_INICIAL|FEC_APROBACION_PENSION)\s*)"
        r"(>=|<=|<)(\s*DATE\s*)"
        r"'(?:\d{4}-\d{2}-\d{2}|\$\{FECHA_INICIO\}|"
        r"\$\{FECHA_SIGUIENTE_MES\}|\$\{FECHA_FIN\})'"
    )

    def reemplazar(match):
        valor = {
            ">=": inicio,
            "<": siguiente,
            "<=": fin
        }[match.group(2)]
        return (
            match.group(1) + match.group(2) + match.group(3)
            + "'" + valor.isoformat() + "'"
        )

    resultado = re.sub(patron, reemplazar, where, flags=re.I)

    # ARP: la tabla pivote debe manejar corte acumulado.
    resultado = re.sub(
        "\\b(?:[A-Za-z_][A-Za-z_0-9]*\\.)?FEC_APROBACION_PENSION\\s*(?:>=|&gt;=)\\s*DATE\\s*'[0-9]{4}-[0-9]{2}-[0-9]{2}'\\s*AND\\s*",
        "", resultado, count=1, flags=re.I
    )

    resultado = re.sub(
        r"(\bPERIODO\s*=\s*)'\d{6}'",
        lambda match: match.group(1) + "'" + nuevo + "'",
        resultado,
        flags=re.I
    )
    return aplicar_periodo_runtime(resultado, viejo, nuevo)

def _preparar_periodo_cnf_base(cnf, event):
    import copy
    import re
    from datetime import date, timedelta

    nuevo = event.get(
        'periodoNegocio',
        cnf['EXTRACCION']['controlExtraccion']['periodo_negocio']
    )
    if not isinstance(nuevo, str) or not re.fullmatch(r'[0-9]{6}', nuevo):
        raise ConfiguracionError('periodoNegocio debe ser YYYYMM')

    try:
        inicio = date(int(nuevo[:4]), int(nuevo[4:]), 1)
        siguiente = date(
            inicio.year + (inicio.month == 12),
            1 if inicio.month == 12 else inicio.month + 1,
            1
        )
    except ValueError:
        raise ConfiguracionError('periodoNegocio contiene mes o anio invalido')

    fin = siguiente - timedelta(days=1)
    antes = copy.deepcopy(cnf)
    salida = copy.deepcopy(cnf)
    viejo = str(cnf['EXTRACCION']['controlExtraccion']['periodo_negocio'])

    control = aplicar_periodo_runtime(
        salida['EXTRACCION']['controlExtraccion'], viejo, nuevo
    )
    control['periodo_negocio'] = nuevo
    salida['EXTRACCION']['controlExtraccion'] = control

    for clave, item in salida.items():
        if not clave.startswith('CICLO#'):
            continue
        for segmento in item.get('segmentos', []):
            for uso in segmento.get('estructuras', []):
                if uso.get('where') is None:
                    continue

                where = uso['where']
                if not isinstance(where, str):
                    raise ConfiguracionError('where debe ser texto: ' + clave)

                where = _resolver_where_periodo(where, viejo, nuevo)

                # Corte de pension identificado en ARP.
                where = re.sub(
                    r"(\bFEC_APROBACION_PENSION\s*<=\s*DATE\s*)'\d{4}-\d{2}-\d{2}'",
                    lambda m: m.group(1) + "'" + fin.isoformat() + "'",
                    where,
                    flags=re.I
                )
                uso['where'] = where

    return salida, antes


def _validar_generados_cnf_base(generados):
    import json
    import re

    for nombre, contenido in generados.items():
        if not isinstance(contenido, dict) or not contenido:
            raise ConfiguracionError('JSON vacio o invalido: ' + nombre)

        texto = json.dumps(normalizar(contenido))
        if re.search(
            r'\$\{(?:PERIODO_NEGOCIO|FECHA_INICIO|FECHA_SIGUIENTE_MES|FECHA_FIN)\}',
            texto
        ):
            raise ConfiguracionError(
                'Marcadores de periodo sin resolver: ' + nombre
            )

    x = generados['EXTRACCION']
    if x['periodo_negocio'] != x['control_extraccion']['periodo_negocio']:
        raise ConfiguracionError('Periodo inconsistente en extraccion')


def _persistir_periodo_cnf(cnf, antes):
    import os
    import json
    from decimal import Decimal
    import boto3
    from boto3.dynamodb.types import TypeSerializer

    serializer = TypeSerializer()

    def serializar(valor):
        return serializer.serialize(
            json.loads(
                json.dumps(valor, default=str),
                parse_float=Decimal
            )
        )

    acciones = []

    for clave, item in cnf.items():
        atributo = (
            'controlExtraccion' if clave == 'EXTRACCION'
            else 'segmentos' if clave.startswith('CICLO#')
            else None
        )
        if atributo is None or item[atributo] == antes[clave][atributo]:
            continue

        acciones.append({
            'Update': {
                'TableName': os.environ['CNF_TABLE'],
                'Key': {
                    'PK': {'S': item['PK']},
                    'SK': {'S': item['SK']}
                },
                'UpdateExpression': 'SET #a = :nuevo',
                'ConditionExpression': '#a = :anterior',
                'ExpressionAttributeNames': {'#a': atributo},
                'ExpressionAttributeValues': {
                    ':nuevo': serializar(item[atributo]),
                    ':anterior': serializar(antes[clave][atributo])
                }
            }
        })

    if acciones:
        if len(acciones) > 100:
            raise ConfiguracionError(
                'Demasiados registros CNF para una transaccion'
            )
        boto3.client('dynamodb').transact_write_items(
            TransactItems=acciones
        )


def _evento_desde_prg(event):
    import os
    import re
    import boto3
    from datetime import datetime
    event = dict(event)
    item = boto3.resource('dynamodb').Table(os.environ['PRG_TABLE']).get_item(
        Key={'PK': os.environ['PRG_PK'], 'SK': os.environ['PRG_SK']},
        ConsistentRead=True
    ).get('Item')
    if not item:
        raise ConfiguracionError('No existe el registro PRG configurado')
    periodo = item.get('periodoNegocio')
    if not isinstance(periodo, str) or not re.fullmatch(r'[0-9]{6}', periodo):
        raise ConfiguracionError('PRG.periodoNegocio debe ser texto YYYYMM')
    try:
        datetime.strptime(periodo, '%Y%m')
    except ValueError:
        raise ConfiguracionError('Periodo PRG invalido')
    if event.get('periodoNegocio') is not None and event['periodoNegocio'] != periodo:
        raise ConfiguracionError('El periodo del evento difiere de PRG; no se publica')
    event['periodoNegocio'] = periodo
    return event


def _preparar_periodo_cnf(cnf, event):
    import re
    salida, antes = _preparar_periodo_cnf_base(cnf, event)
    nuevo = event['periodoNegocio']
    for clave, item in salida.items():
        if not clave.startswith('CICLO#'):
            continue
        for segmento in item.get('segmentos', []):
            for uso in segmento.get('estructuras', []):
                sh = uso.get('segmentacionHorizontal')
                if not isinstance(sh, dict) or not sh:
                    continue
                viejos = set()
                valor = sh.get('periodo_negocio')
                if valor is not None and re.fullmatch(r'[0-9]{6}', str(valor)):
                    viejos.add(str(valor))
                for valor in sh.values():
                    if isinstance(valor, str):
                        viejos.update(re.findall(r'niif17_([0-9]{6})_', valor))
                def actualizar(valor):
                    if isinstance(valor, dict):
                        return {k: actualizar(v) for k, v in valor.items()}
                    if isinstance(valor, list):
                        return [actualizar(v) for v in valor]
                    if isinstance(valor, str):
                        for viejo in sorted(viejos):
                            valor = re.sub(r'(?<![0-9])' + viejo + r'(?![0-9])', nuevo, valor)
                    return valor
                # Solo configuracion de segmentacion; no se tocan objetos de llaves S3.
                uso['segmentacionHorizontal'] = actualizar(sh)
                uso['segmentacionHorizontal']['periodo_negocio'] = nuevo
    return salida, antes


def _validar_generados_cnf(generados):
    import re
    _validar_generados_cnf_base(generados)
    extraccion = generados['EXTRACCION']
    periodo = extraccion['periodo_negocio']
    for ciclo in extraccion.get('ciclos', []):
        for segmento in ciclo.get('segmentos', []):
            for tabla in segmento.get('tablas', []):
                sh = tabla.get('segmentacion_horizontal')
                if not isinstance(sh, dict) or not sh:
                    continue
                if sh.get('periodo_negocio') != periodo:
                    raise ConfiguracionError('Periodo de segmento inconsistente')
                for valor in sh.values():
                    if isinstance(valor, str):
                        periodos = re.findall(r'niif17_([0-9]{6})_', valor)
                        if any(p != periodo for p in periodos):
                            raise ConfiguracionError('Identificador/ruta de segmento inconsistente')
