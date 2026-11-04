#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import boto3
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "USE_JSON_CONFIG",
        "CONFIG_BUCKET",
        "CONFIG_KEY",
        "PERIOD"
    ]
)

USE_JSON_CONFIG = str(args["USE_JSON_CONFIG"]).lower() == "true"
CONFIG_BUCKET = args["CONFIG_BUCKET"]
CONFIG_KEY = args["CONFIG_KEY"]
PERIOD = args["PERIOD"]

if not USE_JSON_CONFIG:
    raise Exception("Este job piloto requiere --USE_JSON_CONFIG true")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3 = boto3.client("s3")


def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_optional_argument(name, default=None):
    flag = f"--{name}"

    if flag not in sys.argv:
        return default

    position = sys.argv.index(flag)

    if position + 1 >= len(sys.argv):
        raise ValueError(
            f"El parametro opcional {flag} no tiene valor."
        )

    value = sys.argv[position + 1].strip()

    if not value:
        return default

    return value


def resolve_fecha_carga():
    fecha_carga = get_optional_argument("FECHA_CARGA")

    if fecha_carga is None:
        return now_ts()

    try:
        datetime.strptime(
            fecha_carga,
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError as error:
        raise ValueError(
            "FECHA_CARGA debe tener formato "
            "YYYY-MM-DD HH:MM:SS. "
            f"Valor recibido: {fecha_carga}"
        ) from error

    return fecha_carga


def read_json_config(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def load_global_config(case_config):
    global_reference = case_config.get(
        "configuracion_global",
        {}
    ) or {}

    global_bucket = global_reference.get(
        "bucket",
        CONFIG_BUCKET
    )

    global_key = global_reference.get("key")

    if not global_key:
        return {}

    return read_json_config(
        global_bucket,
        global_key
    )


def apply_global_defaults(
    case_config,
    global_config
):
    config = dict(case_config)

    comportamiento = global_config.get(
        "comportamiento",
        {}
    ) or {}

    logs_config = global_config.get(
        "logs",
        {}
    ) or {}

    archivo = config.setdefault(
        "archivo",
        {}
    )

    archivo.setdefault(
        "cantidad_archivos_esperada",
        int(
            comportamiento.get(
                "cantidad_archivos_esperada_default",
                1
            )
        )
    )

    rutas = config.setdefault(
        "rutas",
        {}
    )

    if logs_config.get("bucket"):
        rutas.setdefault(
            "bucket_silver",
            logs_config["bucket"]
        )

    if logs_config.get("path"):
        rutas.setdefault(
            "logs_path",
            logs_config["path"]
        )

    salida_bronze = config.setdefault(
        "salida_bronze",
        {}
    )

    salida_bronze.setdefault(
        "modo_escritura",
        comportamiento.get(
            "modo_escritura_bronze_default",
            "overwrite"
        )
    )

    config["_global"] = global_config

    return config


def s3_copy_and_delete(bucket, source_key, dest_key):
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key
    )

    try:
        s3.delete_object(Bucket=bucket, Key=source_key)
    except Exception:
        pass


def infer_regla(columna):
    columna = str(columna or "").lower()

    if "estructura" in columna or "columnas_" in columna:
        return "verificar_estructura_archivo"

    if "archivo" in columna:
        return "verificar_archivo"

    if "lectura_excel" in columna:
        return "lectura_excel"

    if "tipo" in columna:
        return "verificar_tipo_dato"

    if "dominio" in columna:
        return "verificar_dominio"

    if "null" in columna or "nulo" in columna:
        return "verificar_nulidad"

    return "gestor_archivos_json"


def write_validation_logs(
    config,
    errors,
    timestamp,
    received_files=None
):
    if not errors:
        return

    case_use = config["case_use"]
    bucket_silver = config["rutas"]["bucket_silver"]
    logs_path = config["rutas"]["logs_path"].rstrip("/")

    expected_name = config["archivo"]["nombre"]
    expected_extension = config["archivo"]["extension"]
    expected_sheet = config["archivo"]["hoja"]

    received_files = received_files or []

    received_names = [
        item.get("file_name", "")
        for item in received_files
    ]

    rows = []

    for sequence, err in enumerate(errors, start=1):
        columna = str(err.get("columna", ""))

        row = {
            "caso_de_uso": str(case_use),
            "columna": columna,
            "indice": int(err.get("indice", 0)),
            "fecha_ejecucion": str(timestamp),
            "origen_id_lote": int(
                err.get("origen_id_lote", 0)
            ),
            "estado": int(
                err.get(
                    "estado",
                    err.get("cumplio", 0)
                )
            ),
            "fallo": str(err.get("fallo", "")),
            "regla": str(
                err.get(
                    "regla",
                    infer_regla(columna)
                )
            ),

            # Campos normalizados adicionales
            "secuencia_error": int(sequence),
            "tipo_validacion": str(
                err.get(
                    "tipo_validacion",
                    err.get(
                        "regla",
                        infer_regla(columna)
                    )
                )
            ),
            "nombre_archivo_recibido": str(
                err.get(
                    "nombre_archivo_recibido",
                    ", ".join(received_names)
                )
            ),
            "nombre_archivo_esperado": str(
                err.get(
                    "nombre_archivo_esperado",
                    expected_name + expected_extension
                )
            ),
            "extension_recibida": str(
                err.get("extension_recibida", "")
            ),
            "extension_esperada": str(
                err.get(
                    "extension_esperada",
                    expected_extension
                )
            ),
            "hoja_recibida": str(
                err.get("hoja_recibida", "")
            ),
            "hoja_esperada": str(
                err.get(
                    "hoja_esperada",
                    expected_sheet
                )
            ),
            "posicion_esperada": int(
                err.get("posicion_esperada", 0)
            ),
            "posicion_recibida": int(
                err.get("posicion_recibida", 0)
            ),
            "valor_recibido": str(
                err.get("valor_recibido", "")
            ),
            "ruta_archivo_error": str(
                err.get("ruta_archivo_error", "")
            )
        }

        rows.append(row)

    df_log = spark.createDataFrame(rows)

    output_path = (
        f"s3://{bucket_silver}/{logs_path}/"
    )

    df_log.coalesce(1).write.mode(
        "append"
    ).parquet(output_path)

    print(
        f"Total errores escritos en logs: {len(rows)}"
    )
    print(f"Ruta logs: {output_path}")


def list_input_objects(bucket, prefix):
    normalized_prefix = prefix.rstrip("/") + "/"
    objects = []
    continuation_token = None

    while True:
        request = {
            "Bucket": bucket,
            "Prefix": normalized_prefix
        }

        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**request)

        for item in response.get("Contents", []):
            key = item["Key"]

            if key.endswith("/"):
                continue

            objects.append({
                "key": key,
                "file_name": key.rsplit("/", 1)[-1],
                "size": int(item.get("Size", 0))
            })

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get(
            "NextContinuationToken"
        )

    return objects


def build_error(
    columna,
    fallo,
    indice=0,
    regla=None,
    **details
):
    error = {
        "columna": str(columna or ""),
        "indice": int(indice or 0),
        "fallo": str(fallo or ""),
        "cumplio": 0
    }

    if regla:
        error["regla"] = regla

    for key, value in details.items():
        error[key] = value

    return error


def build_error_destination(
    landing_error,
    timestamp,
    received_file_name
):
    base_name = received_file_name.rsplit(".", 1)[0]

    return (
        f"{landing_error}/{base_name}/"
        f"fecha_carga={timestamp}/"
        f"{received_file_name}"
    )


def move_objects_to_error(
    bucket,
    objects,
    landing_error,
    timestamp
):
    moved = []

    for item in objects:
        source_key = item["key"]
        received_file_name = item["file_name"]

        destination_key = build_error_destination(
            landing_error,
            timestamp,
            received_file_name
        )

        s3_copy_and_delete(
            bucket,
            source_key,
            destination_key
        )

        moved.append({
            "source_key": source_key,
            "destination_key": destination_key
        })

        print(
            "Archivo movido a ERROR: "
            f"s3://{bucket}/{destination_key}"
        )

    return moved


def validate_input_inventory(
    objects,
    expected_file_name,
    expected_extension
):
    errors = []

    expected_full_name = (
        f"{expected_file_name}{expected_extension}"
    )

    if not objects:
        errors.append(
            build_error(
                columna="archivo_no_encontrado",
                fallo=(
                    "No se encontraron archivos en la ruta IN. "
                    f"Archivo esperado: {expected_full_name}"
                ),
                regla="verificar_archivo",
                nombre_archivo_esperado=expected_full_name
            )
        )

        return errors

    if len(objects) > 1:
        received_names = [
            item["file_name"]
            for item in objects
        ]

        errors.append(
            build_error(
                columna="cantidad_archivos",
                fallo=(
                    "Se esperaba un solo archivo en IN, "
                    f"pero se encontraron {len(objects)}: "
                    + ", ".join(received_names)
                ),
                regla="verificar_archivo",
                cantidad_archivos_recibidos=len(objects)
            )
        )

    for item in objects:
        received_name = item["file_name"]

        if "." in received_name:
            received_base, received_extension = (
                "." + received_name.rsplit(".", 1)[1]
            ), None

            received_base = received_name[
                :-(len(received_base))
            ]
            received_extension = (
                "." + received_name.rsplit(".", 1)[1]
            )
        else:
            received_base = received_name
            received_extension = ""

        if received_base != expected_file_name:
            errors.append(
                build_error(
                    columna="nombre_archivo",
                    fallo=(
                        "Nombre de archivo incorrecto. "
                        f"Esperado: {expected_file_name}. "
                        f"Recibido: {received_base}"
                    ),
                    regla="verificar_nombre_archivo",
                    nombre_archivo_recibido=received_name,
                    nombre_archivo_esperado=expected_full_name
                )
            )

        if received_extension.lower() != (
            expected_extension.lower()
        ):
            errors.append(
                build_error(
                    columna="extension_archivo",
                    fallo=(
                        "Extensión de archivo incorrecta. "
                        f"Esperada: {expected_extension}. "
                        f"Recibida: "
                        f"{received_extension or 'SIN_EXTENSION'}"
                    ),
                    regla="verificar_extension",
                    nombre_archivo_recibido=received_name,
                    extension_recibida=(
                        received_extension
                        or "SIN_EXTENSION"
                    ),
                    extension_esperada=expected_extension
                )
            )

        if received_name != expected_full_name:
            errors.append(
                build_error(
                    columna="archivo_esperado",
                    fallo=(
                        "El archivo recibido no coincide con "
                        f"el archivo esperado: {expected_full_name}. "
                        f"Recibido: {received_name}"
                    ),
                    regla="verificar_archivo",
                    nombre_archivo_recibido=received_name,
                    nombre_archivo_esperado=expected_full_name
                )
            )

    return errors


def validate_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def download_file(bucket, key, local_path):
    s3.download_file(bucket, key, local_path)


def validate_structure(df, expected_columns):
    errors = []

    actual_columns = list(df.columns)

    missing = [c for c in expected_columns if c not in actual_columns]
    extra = [c for c in actual_columns if c not in expected_columns]

    if missing:
        errors.append({
            "columna": "columnas_faltantes",
            "indice": 0,
            "fallo": "Faltan columnas requeridas: " + ", ".join(missing),
            "cumplio": 0
        })

    if extra:
        errors.append({
            "columna": "columnas_extra",
            "indice": 0,
            "fallo": "Columnas adicionales no esperadas: " + ", ".join(extra),
            "cumplio": 0
        })

    for idx, col in enumerate(expected_columns):
        actual = actual_columns[idx] if idx < len(actual_columns) else "N/A"

        if actual != col:
            errors.append({
                "columna": f"estructura_pos_{idx}",
                "indice": 0,
                "fallo": f"Columna esperada en posición {idx}: '{col}', encontrada: '{actual}'",
                "cumplio": 0
            })

    if errors:
        errors.append({
            "columna": "estructura_archivo",
            "indice": 0,
            "fallo": f"Estructura de archivo inválida. Errores encontrados: {len(errors)}",
            "cumplio": 0
        })

    return errors


def validate_data(df, columnas_config):
    errors = []

    for col_cfg in columnas_config:
        col = col_cfg["nombre"]
        tipo = col_cfg.get("tipo", "string").lower()
        obligatorio = bool(
            col_cfg.get("obligatorio", False)
        )
        dominio = col_cfg.get("dominio")

        # Una columna inexistente ya se reporta
        # mediante validate_structure().
        if col not in df.columns:
            continue

        serie = df[col]

        mask_empty = (
            serie.isna()
            | (
                serie.fillna("")
                .astype(str)
                .str.strip()
                == ""
            )
        )

        # -----------------------------------------
        # NULIDAD: registrar todas las filas
        # -----------------------------------------
        if obligatorio:
            invalid_indexes = serie.index[mask_empty]

            for idx in invalid_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=(
                            "Campo obligatorio con valor "
                            f"nulo o vacío en columna {col}"
                        ),
                        regla="verificar_nulidad",
                        tipo_validacion="nulidad",
                        valor_recibido=""
                    )
                )

        # Los vacíos obligatorios ya fueron registrados.
        # Los vacíos opcionales no se validan por tipo.
        mask_with_value = ~mask_empty

        # -----------------------------------------
        # ENTEROS
        # -----------------------------------------
        if tipo in ["int", "integer"]:
            numeric = pd.to_numeric(
                serie,
                errors="coerce"
            )

            mask_bad = (
                mask_with_value
                & numeric.isna()
            )

            invalid_indexes = serie.index[mask_bad]

            for idx in invalid_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=(
                            "Tipo inválido. Se esperaba "
                            f"entero en columna {col}"
                        ),
                        regla="verificar_tipo_dato",
                        tipo_validacion="tipo_dato",
                        valor_recibido=str(
                            serie.loc[idx]
                        )
                    )
                )

            mask_decimal = (
                mask_with_value
                & numeric.notna()
                & ((numeric % 1) != 0)
            )

            decimal_indexes = serie.index[mask_decimal]

            for idx in decimal_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=(
                            "Tipo inválido. Se esperaba "
                            "entero sin decimales en "
                            f"columna {col}"
                        ),
                        regla="verificar_tipo_dato",
                        tipo_validacion="tipo_dato",
                        valor_recibido=str(
                            serie.loc[idx]
                        )
                    )
                )

        # -----------------------------------------
        # DECIMALES
        # -----------------------------------------
        elif tipo in [
            "decimal",
            "double",
            "float",
            "numeric"
        ]:
            numeric = pd.to_numeric(
                serie,
                errors="coerce"
            )

            mask_bad = (
                mask_with_value
                & numeric.isna()
            )

            invalid_indexes = serie.index[mask_bad]

            for idx in invalid_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=(
                            "Tipo inválido. Se esperaba "
                            f"decimal en columna {col}"
                        ),
                        regla="verificar_tipo_dato",
                        tipo_validacion="tipo_dato",
                        valor_recibido=str(
                            serie.loc[idx]
                        )
                    )
                )

        # -----------------------------------------
        # FECHAS
        # -----------------------------------------
        elif tipo in [
            "date",
            "fecha",
            "timestamp",
            "datetime"
        ]:
            parsed = pd.to_datetime(
                serie,
                errors="coerce"
            )

            mask_bad = (
                mask_with_value
                & parsed.isna()
            )

            invalid_indexes = serie.index[mask_bad]

            for idx in invalid_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=(
                            "Tipo inválido. Se esperaba "
                            f"fecha en columna {col}"
                        ),
                        regla="verificar_tipo_dato",
                        tipo_validacion="tipo_dato",
                        valor_recibido=str(
                            serie.loc[idx]
                        )
                    )
                )

        # -----------------------------------------
        # PATRON DE CARACTERES
        # -----------------------------------------
        patron = col_cfg.get("patron")
        mensaje_patron = col_cfg.get(
            "mensaje_patron",
            f"Valor inválido según el patrón configurado para columna {col}"
        )

        if patron:
            try:
                mask_bad_pattern = (
                    mask_with_value
                    & ~serie.astype(str).str.fullmatch(
                        patron,
                        na=False
                    )
                )
            except Exception as error:
                raise ValueError(
                    f"Patrón inválido configurado para columna {col}: "
                    f"{patron}. Error: {error}"
                ) from error

            invalid_pattern_indexes = serie.index[
                mask_bad_pattern
            ]

            for idx in invalid_pattern_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=mensaje_patron,
                        regla="verificar_patron",
                        tipo_validacion="patron",
                        valor_recibido=str(
                            serie.loc[idx]
                        )
                    )
                )

        # -----------------------------------------
        # DOMINIO
        # -----------------------------------------
        if dominio is not None:
            allowed = {
                str(value).strip()
                for value in dominio
            }

            normalized = (
                serie.fillna("")
                .astype(str)
                .str.strip()
            )

            mask_domain = (
                mask_with_value
                & ~normalized.isin(allowed)
            )

            invalid_indexes = serie.index[mask_domain]

            for idx in invalid_indexes:
                errors.append(
                    build_error(
                        columna=col,
                        indice=int(idx) + 2,
                        fallo=(
                            "Valor fuera de dominio en "
                            f"columna {col}. "
                            f"Dominio permitido: {dominio}"
                        ),
                        regla="verificar_dominio",
                        tipo_validacion="dominio",
                        valor_recibido=str(
                            serie.loc[idx]
                        )
                    )
                )

    return errors


def resolve_periodo_archivo(df, config):
    periodo_column = config.get(
        "parametros_ejecucion",
        {}
    ).get(
        "columna_periodo",
        "MES_DE_CIERRE"
    )

    if periodo_column not in df.columns:
        return None, [
            build_error(
                columna=periodo_column,
                fallo=(
                    "No existe la columna configurada "
                    f"para obtener el periodo: {periodo_column}"
                ),
                regla="verificar_periodo",
                tipo_validacion="periodo"
            )
        ]

    values = (
        df[periodo_column]
        .dropna()
        .astype(str)
        .str.strip()
    )

    values = [
        value
        for value in values.unique().tolist()
        if value
    ]

    errors = []

    invalid_values = [
        value
        for value in values
        if len(value) != 6 or not value.isdigit()
    ]

    for value in invalid_values:
        errors.append(
            build_error(
                columna=periodo_column,
                fallo=(
                    "Periodo inválido. Se esperaba "
                    f"formato YYYYMM y se recibió: {value}"
                ),
                regla="verificar_periodo",
                tipo_validacion="periodo",
                valor_recibido=value
            )
        )

    valid_values = [
        value
        for value in values
        if len(value) == 6 and value.isdigit()
    ]

    if not valid_values:
        errors.append(
            build_error(
                columna=periodo_column,
                fallo=(
                    "No se encontró un periodo YYYYMM "
                    "válido en el archivo."
                ),
                regla="verificar_periodo",
                tipo_validacion="periodo"
            )
        )

    if len(set(valid_values)) > 1:
        errors.append(
            build_error(
                columna=periodo_column,
                fallo=(
                    "El archivo contiene más de un periodo: "
                    + ", ".join(sorted(set(valid_values)))
                ),
                regla="verificar_periodo",
                tipo_validacion="periodo",
                valor_recibido=", ".join(
                    sorted(set(valid_values))
                )
            )
        )

    if errors:
        return None, errors

    return valid_values[0], []


def write_execution_metadata(
    bucket,
    landing_base,
    timestamp,
    case_use,
    periodo
):
    metadata_key = (
        f"{landing_base}/METADATA/"
        f"fecha_carga={timestamp}/execution.json"
    )

    body = {
        "case_use": case_use,
        "periodo": periodo,
        "fecha_carga": timestamp,
        "estado": "VALIDADO"
    }

    s3.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(
            body,
            ensure_ascii=False
        ).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        "Metadata ejecución: "
        f"s3://{bucket}/{metadata_key}"
    )

    return metadata_key


def prepare_output_df(df, config):
    columnas_config = sorted(
        config["columnas"],
        key=lambda x: x["orden"]
    )

    # Los nombres de origen corresponden a los encabezados del Excel.
    source_columns = [
        c.get("nombre_origen", c.get("nombre"))
        for c in columnas_config
    ]

    out = df[source_columns].copy()

    # Renombrar únicamente para la salida Bronze.
    rename_map = {
        c.get("nombre_origen", c.get("nombre")):
        c.get(
            "nombre_destino",
            c.get("nombre_origen", c.get("nombre"))
        )
        for c in columnas_config
    }

    out = out.rename(columns=rename_map)

    salida_config = config.get("salida_bronze", {})

    no_registro_col = salida_config.get(
        "columna_no_registro",
        "NO_REGISTRO"
    )

    lote_cargue_col = salida_config.get(
        "columna_lote_cargue",
        "ID_LOTE_CARGUE"
    )

    agregar_no_registro = salida_config.get(
        "agregar_no_registro",
        True
    )

    agregar_lote_cargue = salida_config.get(
        "agregar_id_lote_cargue",
        True
    )

    if agregar_no_registro:
        out.insert(
            0,
            no_registro_col,
            range(2, 2 + len(out))
        )

    if agregar_lote_cargue:
        out[lote_cargue_col] = int(
            salida_config.get("valor_lote_cargue", 0)
        )

    for col_cfg in columnas_config:
        source_col = col_cfg.get(
            "nombre_origen",
            col_cfg.get("nombre")
        )

        target_col = col_cfg.get(
            "nombre_destino",
            source_col
        )

        tipo = col_cfg.get(
            "tipo",
            "string"
        ).lower()

        if tipo in ["int", "integer"]:
            out[target_col] = (
                pd.to_numeric(
                    out[target_col],
                    errors="coerce"
                )
                .astype("Int64")
            )

        elif tipo in ["decimal", "double", "float", "numeric"]:
            out[target_col] = pd.to_numeric(
                out[target_col],
                errors="coerce"
            )

        elif tipo in ["date", "fecha"]:
            out[target_col] = (
                pd.to_datetime(
                    out[target_col],
                    errors="coerce"
                )
                .dt.date
            )

        elif tipo in ["timestamp", "datetime"]:
            out[target_col] = pd.to_datetime(
                out[target_col],
                errors="coerce"
            )

        else:
            out[target_col] = (
                out[target_col]
                .astype("string")
            )

    output_columns = salida_config.get("columnas")

    if output_columns:
        missing_output_columns = [
            col
            for col in output_columns
            if col not in out.columns
        ]

        if missing_output_columns:
            raise ValueError(
                "Columnas Bronze configuradas que no fueron generadas: "
                + ", ".join(missing_output_columns)
            )

        out = out[output_columns]

    return out

def main():
    timestamp = resolve_fecha_carga()

    print(
        f"FECHA_CARGA utilizada por el Gestor: "
        f"{timestamp}"
    )

    case_config = read_json_config(
        CONFIG_BUCKET,
        CONFIG_KEY
    )

    global_config = load_global_config(
        case_config
    )

    config = apply_global_defaults(
        case_config,
        global_config
    )

    global_reference = case_config.get(
        "configuracion_global",
        {}
    ) or {}

    print(
        "Configuracion global Gestor: "
        + str(
            global_reference.get(
                "key",
                "NO_CONFIGURADA"
            )
        )
    )

    case_use = config["case_use"]

    file_name = config["archivo"]["nombre"]
    extension = config["archivo"]["extension"]
    sheet_name = config["archivo"]["hoja"]

    expected_full_name = (
        f"{file_name}{extension}"
    )

    bucket_landing = config["rutas"]["bucket_landing"]
    bucket_bronze = config["rutas"]["bucket_bronze"]

    landing_in = (
        config["rutas"]["landing_in"]
        .strip("/")
    )

    landing_out = (
        config["rutas"]["landing_out"]
        .strip("/")
    )

    landing_error = (
        config["rutas"]["landing_error"]
        .strip("/")
    )

    bronze_path = (
        config["rutas"]["bronze_path"]
        .strip("/")
    )

    source_key = (
        f"{landing_in}/{expected_full_name}"
    )

    local_file = (
        f"/tmp/{expected_full_name}"
    )

    errors = []

    # ---------------------------------------------
    # INVENTARIO COMPLETO DE IN
    # ---------------------------------------------
    input_objects = list_input_objects(
        bucket_landing,
        landing_in
    )

    print(
        "Archivos encontrados en IN: "
        f"{len(input_objects)}"
    )

    for item in input_objects:
        print(
            f"IN: s3://{bucket_landing}/"
            f"{item['key']}"
        )

    inventory_errors = validate_input_inventory(
        input_objects,
        file_name,
        extension
    )

    errors.extend(inventory_errors)

    exact_objects = [
        item
        for item in input_objects
        if item["file_name"] == expected_full_name
    ]

    # Si el inventario tiene errores, se rechaza
    # la ejecución completa y se mueven todos los
    # archivos recibidos a ERROR.
    if errors:
        moved = move_objects_to_error(
            bucket_landing,
            input_objects,
            landing_error,
            timestamp
        )

        error_paths = {
            item["source_key"]:
            item["destination_key"]
            for item in moved
        }

        for error in errors:
            received_name = error.get(
                "nombre_archivo_recibido",
                ""
            )

            for item in input_objects:
                if (
                    not received_name
                    or item["file_name"]
                    in received_name
                ):
                    destination = error_paths.get(
                        item["key"],
                        ""
                    )

                    if destination:
                        error["ruta_archivo_error"] = (
                            f"s3://{bucket_landing}/"
                            f"{destination}"
                        )
                        break

        write_validation_logs(
            config,
            errors,
            timestamp,
            input_objects
        )

        print(
            "Archivo(s) rechazado(s) por validaciones "
            "de nombre, extensión o cantidad."
        )

        for error in errors:
            print(error)

        return

    if len(exact_objects) != 1:
        unexpected_error = build_error(
            columna="archivo_verificacion",
            fallo=(
                "No fue posible determinar un único "
                f"archivo procesable: {expected_full_name}"
            ),
            regla="verificar_archivo",
            nombre_archivo_esperado=expected_full_name
        )

        write_validation_logs(
            config,
            [unexpected_error],
            timestamp,
            input_objects
        )

        raise Exception(
            unexpected_error["fallo"]
        )

    # ---------------------------------------------
    # DESCARGA Y LECTURA DEL LIBRO
    # ---------------------------------------------
    download_file(
        bucket_landing,
        source_key,
        local_file
    )

    try:
        excel_file = pd.ExcelFile(local_file)
        sheet_names = list(excel_file.sheet_names)

        if sheet_name not in sheet_names:
            errors.append(
                build_error(
                    columna="hoja_excel",
                    fallo=(
                        "Hoja requerida no encontrada. "
                        f"Esperada: {sheet_name}. "
                        "Hojas recibidas: "
                        + ", ".join(sheet_names)
                    ),
                    regla="verificar_hoja",
                    tipo_validacion="hoja",
                    hoja_recibida=", ".join(
                        sheet_names
                    ),
                    hoja_esperada=sheet_name,
                    nombre_archivo_recibido=(
                        expected_full_name
                    )
                )
            )

            raise KeyError(
                f"Hoja no encontrada: {sheet_name}"
            )

        df = pd.read_excel(
            local_file,
            sheet_name=sheet_name,
            dtype=str
        )

    except Exception as error:
        if not errors:
            errors.append(
                build_error(
                    columna="lectura_excel",
                    fallo=(
                        "Error leyendo archivo Excel: "
                        f"{str(error)}"
                    ),
                    regla="lectura_excel",
                    tipo_validacion="lectura_excel",
                    nombre_archivo_recibido=(
                        expected_full_name
                    ),
                    hoja_esperada=sheet_name
                )
            )

        destination_key = build_error_destination(
            landing_error,
            timestamp,
            expected_full_name
        )

        s3_copy_and_delete(
            bucket_landing,
            source_key,
            destination_key
        )

        for item in errors:
            item["ruta_archivo_error"] = (
                f"s3://{bucket_landing}/"
                f"{destination_key}"
            )

        write_validation_logs(
            config,
            errors,
            timestamp,
            input_objects
        )

        print(
            "Archivo rechazado por lectura "
            "o validación de hoja."
        )

        for item in errors:
            print(item)

        return

    # ---------------------------------------------
    # ESTRUCTURA Y DATOS
    # ---------------------------------------------
    expected_columns = [
        column["nombre"]
        for column in sorted(
            config["columnas"],
            key=lambda item: item["orden"]
        )
    ]

    structure_errors = validate_structure(
        df,
        expected_columns
    )

    data_errors = validate_data(
        df,
        config["columnas"]
    )

    execution_config = config.get(
        "parametros_ejecucion",
        {}
    ) or {}

    periodo_config = execution_config.get(
        "periodo",
        {}
    ) or {}

    periodo_aplica = bool(
        periodo_config.get(
            "aplica",
            False
        )
    )

    periodo_column = periodo_config.get(
        "columna_origen"
    )

    if periodo_aplica and periodo_column:
        config.setdefault(
            "parametros_ejecucion",
            {}
        )["columna_periodo"] = periodo_column

        periodo_archivo, periodo_errors = (
            resolve_periodo_archivo(
                df,
                config
            )
        )
    else:
        periodo_archivo = "NO_APLICA"
        periodo_errors = []

    errors.extend(structure_errors)
    errors.extend(data_errors)
    errors.extend(periodo_errors)

    if errors:
        destination_key = build_error_destination(
            landing_error,
            timestamp,
            expected_full_name
        )

        s3_copy_and_delete(
            bucket_landing,
            source_key,
            destination_key
        )

        for item in errors:
            item["ruta_archivo_error"] = (
                f"s3://{bucket_landing}/"
                f"{destination_key}"
            )

        write_validation_logs(
            config,
            errors,
            timestamp,
            input_objects
        )

        print(
            "Archivo rechazado por validaciones. "
            f"Total errores: {len(errors)}"
        )

        for item in errors:
            print(item)

        return

    # ---------------------------------------------
    # SALIDA CORRECTA
    # ---------------------------------------------
    output_pd = prepare_output_df(
        df,
        config
    )

    output_spark = spark.createDataFrame(
        output_pd
    )

    output_path = (
        f"s3://{bucket_bronze}/"
        f"{bronze_path}/"
        f"fecha_carga={timestamp}/"
    )

    bronze_write_mode = (
        config.get(
            "salida_bronze",
            {}
        ).get(
            "modo_escritura",
            "overwrite"
        )
    )

    output_spark.write.mode(
        bronze_write_mode
    ).parquet(output_path)

    destination_key = (
        f"{landing_out}/{file_name}/"
        f"fecha_carga={timestamp}/"
        f"{expected_full_name}"
    )

    s3_copy_and_delete(
        bucket_landing,
        source_key,
        destination_key
    )

    landing_base = landing_in.rsplit(
        "/",
        1
    )[0]

    execution_config = config.get(
        "parametros_ejecucion",
        {}
    ) or {}

    generar_metadata = bool(
        execution_config.get(
            "generar_metadata",
            False
        )
    )

    if generar_metadata:
        write_execution_metadata(
            bucket=bucket_landing,
            landing_base=landing_base,
            timestamp=timestamp,
            case_use=case_use,
            periodo=periodo_archivo
        )

    print("Archivo procesado correctamente.")
    print(f"Case use: {case_use}")
    print(f"Bronze path: {output_path}")
    print(
        "OUT path: "
        f"s3://{bucket_landing}/"
        f"{destination_key}"
    )


if __name__ == "__main__":
    main()
