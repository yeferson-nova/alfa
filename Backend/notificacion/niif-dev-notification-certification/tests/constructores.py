import copy
from typing import Any

from notificacion_certificacion.dominio.modelos import (
    ParametrosEjecucion,
    RegistroCompuesto,
    RegistroNotificacion,
    ResultadoConsulta,
)

CONFIG_GLOBAL: dict[str, Any] = {
    "buckets": {"assets": "bucket-activos"},
    "dynamodb": {"notification_table": "tabla-notificaciones"},
    "environment": "dev",
    "athena": {"output_path": "athena/salida"},
    "certification": {"sent_status": "SENT"},
}

CONFIG_CASO: dict[str, Any] = {
    "useCaseId": "casoA",
    "email": {
        "subject": "NIIF 17 - Certificacion ${useCaseName} Periodo ${period}",
        "stringVars": {
            "useCaseName": "Caso A",
            "certificarLink": "https://api.example/certify?c=${certCode}&amp;p=${period}",
            "nocertificarLink": "https://api.example/no?c=${certCode}",
        },
        "destinations": ["Uno@Example.com", " dos@example.com ", "uno@example.com"],
    },
    "queries": {
        "summary": [
            {"queryName": "b_resumen", "database": "db_${env}", "query": "SELECT 'b' FROM t WHERE p='${period}'"},
            {"queryName": "a_resumen", "database": "db_${env}", "query": "SELECT 'a' FROM t WHERE p='${period}'"},
        ],
        "total": [
            {"queryName": "total", "database": "db_${env}", "query": "SELECT 'total' FROM t WHERE p='${period}'"},
        ],
    },
}

RESPUESTAS_POR_DEFECTO: dict[str, ResultadoConsulta] = {
    "SELECT 'a' FROM t WHERE p='202610'": ResultadoConsulta(("periodo", "total"), (("202610", "5"),)),
    "SELECT 'b' FROM t WHERE p='202610'": ResultadoConsulta(("periodo", "total"), (("202610", "7"),)),
    "SELECT 'total' FROM t WHERE p='202610'": ResultadoConsulta(("registros", "valor"), (("12", "900"),)),
}


def construir_parametros(**cambios: Any) -> ParametrosEjecucion:
    base: dict[str, Any] = {
        "caso_uso": "casoA",
        "cert_code": "CERT1",
        "tipo_correo": "certification",
        "bucket_configuracion": "bucket-config",
        "clave_caso": "casos/casoA.json",
        "clave_global": "global.json",
        "url_redireccion": "https://redir.example/r",
        "tabla_enlaces": "tabla-enlaces",
        "remitente": "no-responder@example.com",
        "conjunto_configuracion": None,
        "indice_cert_code": "indice-cert",
        "zona_horaria": "America/Bogota",
        "dias_vigencia_enlace": 7,
        "longitud_token_enlace": 8,
        "segundos_entre_consultas": 1.0,
        "segundos_maximos_consulta": 60.0,
        "regeneracion_sin_datos_nuevos": False,
        "id_ejecucion": "ejec-1",
    }
    base.update(cambios)
    return ParametrosEjecucion(**base)


def construir_registro(**cambios: Any) -> RegistroNotificacion:
    base: dict[str, Any] = {
        "caso_uso": "casoA",
        "id_ejecucion": "ej1",
        "periodo": "202610",
        "estado": "PENDING",
        "correo_usuario": "usuario@example.com",
        "nombre_consulta": "detail#a",
        "nombre_archivo": "a.csv",
        "url_s3": "https://s3.example/a.csv?x=1&amp;y=2",
        "sin_datos_periodo": False,
    }
    base.update(cambios)
    return RegistroNotificacion(**base)


def construir_registro_compuesto(**cambios: Any) -> RegistroCompuesto:
    base: dict[str, Any] = {
        "id_miembro": "m1",
        "estado": "READY_TO_NOTIFY",
        "periodo": "202610",
        "cert_code": "",
        "id_ejecucion_origen": "origen-1",
        "id_lote": "lote-1",
        "actualizado_en": "2026-11-04T10:00:00",
    }
    base.update(cambios)
    return RegistroCompuesto(**base)


def copiar_config_caso() -> dict[str, Any]:
    return copy.deepcopy(CONFIG_CASO)


def config_compuesta() -> dict[str, Any]:
    return {
        "enabled": True,
        "tableName": "tabla-compuesto",
        "groupId": "grupo",
        "memberIds": ["m1", "m2"],
    }
