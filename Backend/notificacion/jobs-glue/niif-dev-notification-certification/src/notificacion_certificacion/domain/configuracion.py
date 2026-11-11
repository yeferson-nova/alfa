from typing import Any, Mapping

from notificacion_certificacion.domain.constantes import (
    LONGITUD_MAXIMA_TOKEN,
    TIPO_CONSULTA_RESUMEN,
    TIPO_CONSULTA_TOTALES,
)
from notificacion_certificacion.domain.destinatarios import normalizar_destinatarios
from notificacion_certificacion.domain.errores import ErrorConfiguracion
from notificacion_certificacion.domain.modelos import (
    AjustesNotificacion,
    ConfiguracionCaso,
    ConfiguracionCompuesta,
    ConfiguracionGlobal,
    Consulta,
    PoliticaEnlaces,
)

SECCION_NOTIFICACION = "notification"


def _existe(datos: Any, ruta: tuple[str, ...]) -> bool:
    actual = datos
    for clave in ruta:
        if not isinstance(actual, Mapping) or clave not in actual:
            return False
        actual = actual[clave]
    return True


def _buscar(datos: Any, ruta: tuple[str, ...]) -> Any:
    if not _existe(datos, ruta):
        raise ErrorConfiguracion("NTF-CFG-001", f"Falta la clave de configuracion: {'.'.join(ruta)}")
    actual = datos
    for clave in ruta:
        actual = actual[clave]
    return actual


def _texto(datos: Any, *ruta: str) -> str:
    valor = _buscar(datos, ruta)
    if not isinstance(valor, str) or not valor.strip():
        raise ErrorConfiguracion("NTF-CFG-001", f"La clave debe ser texto no vacio: {'.'.join(ruta)}")
    return valor


def _texto_o_nulo(datos: Any, *ruta: str) -> str | None:
    valor = _buscar(datos, ruta)
    if valor is None:
        return None
    return _texto(datos, *ruta).strip()


def _numero(datos: Any, *ruta: str) -> float:
    valor = _buscar(datos, ruta)
    if isinstance(valor, bool) or not isinstance(valor, (int, float)) or valor <= 0:
        raise ErrorConfiguracion("NTF-CFG-001", f"La clave debe ser un numero mayor a cero: {'.'.join(ruta)}")
    return float(valor)


def _entero(datos: Any, *ruta: str) -> int:
    valor = _buscar(datos, ruta)
    if isinstance(valor, bool) or not isinstance(valor, int) or valor <= 0:
        raise ErrorConfiguracion("NTF-CFG-001", f"La clave debe ser un entero mayor a cero: {'.'.join(ruta)}")
    return valor


def _lista(datos: Any, ruta: tuple[str, ...], codigo: str) -> list[Any]:
    if not _existe(datos, ruta):
        return []
    valor = _buscar(datos, ruta)
    if not isinstance(valor, list):
        raise ErrorConfiguracion(codigo, f"La clave debe ser una lista: {'.'.join(ruta)}")
    return valor


def _politica_enlaces(datos: Any) -> PoliticaEnlaces:
    longitud = _entero(datos, SECCION_NOTIFICACION, "link_token_length")
    if longitud > LONGITUD_MAXIMA_TOKEN:
        raise ErrorConfiguracion(
            "NTF-CFG-001", f"notification.link_token_length no puede ser mayor a {LONGITUD_MAXIMA_TOKEN}"
        )
    return PoliticaEnlaces(
        url_base=_texto(datos, SECCION_NOTIFICACION, "redirect_base_url").strip(),
        tabla=_texto(datos, SECCION_NOTIFICACION, "short_links_table").strip(),
        dias_vigencia=_entero(datos, SECCION_NOTIFICACION, "link_validity_days"),
        longitud_token=longitud,
    )


def _ajustes_notificacion(datos: Any) -> AjustesNotificacion:
    entre_consultas = _numero(datos, SECCION_NOTIFICACION, "athena_poll_seconds")
    maximo_consulta = _numero(datos, SECCION_NOTIFICACION, "athena_max_wait_seconds")
    if maximo_consulta < entre_consultas:
        raise ErrorConfiguracion(
            "NTF-CFG-001", "notification.athena_max_wait_seconds no puede ser menor a athena_poll_seconds"
        )
    return AjustesNotificacion(
        indice_cert_code=_texto(datos, "dynamodb", "cert_code_index").strip(),
        remitente=_texto(datos, SECCION_NOTIFICACION, "ses_sender").strip(),
        conjunto_configuracion=_texto_o_nulo(datos, SECCION_NOTIFICACION, "ses_configuration_set"),
        enlaces=_politica_enlaces(datos),
        segundos_entre_consultas=entre_consultas,
        segundos_maximos_consulta=maximo_consulta,
    )


def leer_configuracion_global(datos: Any) -> ConfiguracionGlobal:
    return ConfiguracionGlobal(
        bucket_activos=_texto(datos, "buckets", "assets"),
        tabla_notificaciones=_texto(datos, "dynamodb", "notification_table"),
        catalogo=_texto(datos, "catalog"),
        ambiente=_texto(datos, "environment"),
        prefijo_salida_athena=_texto(datos, "athena", "output_path"),
        estado_enviado=_texto(datos, "certification", "sent_status"),
        zona_horaria=_texto(datos, "timezone").strip(),
        notificacion=_ajustes_notificacion(datos),
    )


def _consulta(cruda: Any, ruta: str) -> Consulta:
    if not isinstance(cruda, Mapping):
        raise ErrorConfiguracion("NTF-CFG-001", f"La consulta debe ser un objeto: {ruta}")
    return Consulta(
        nombre=str(cruda.get("queryName", "")),
        base_datos=_texto(cruda, "database"),
        sql=_texto(cruda, "query"),
    )


def _consultas(datos: Any, tipo: str) -> tuple[Consulta, ...]:
    crudas = _lista(datos, ("queries", tipo), "NTF-CFG-001")
    consultas = (_consulta(cruda, f"queries.{tipo}") for cruda in crudas)
    return tuple(sorted(consultas, key=lambda consulta: consulta.nombre))


def _miembro_presencia(miembro: Any) -> Consulta:
    nombre = miembro.get("name") if isinstance(miembro, Mapping) else None
    if not isinstance(nombre, str) or not nombre.strip():
        raise ErrorConfiguracion("NTF-CFG-005", "Nombre de miembro ausente en dataPresence")
    return Consulta(nombre=nombre, base_datos=_texto(miembro, "database"), sql=_texto(miembro, "query"))


def _presencia(correo: Any) -> tuple[Consulta, ...]:
    miembros = _lista(correo, ("dataPresence",), "NTF-CFG-005")
    consultas = tuple(_miembro_presencia(miembro) for miembro in miembros)
    nombres = [consulta.nombre for consulta in consultas]
    if len(set(nombres)) != len(nombres):
        raise ErrorConfiguracion("NTF-CFG-005", "Nombre de miembro duplicado en dataPresence")
    return consultas


def _compuesta(datos: Any) -> ConfiguracionCompuesta | None:
    cruda = datos.get("compositeNotification") if isinstance(datos, Mapping) else None
    if not cruda or not cruda.get("enabled", False):
        return None
    miembros = tuple(_lista(cruda, ("memberIds",), "NTF-CFG-004"))
    control = _texto(cruda, "controlMemberId")
    if not miembros or len(set(miembros)) != len(miembros) or control in miembros:
        raise ErrorConfiguracion("NTF-CFG-004", "Miembros del compuesto invalidos")
    return ConfiguracionCompuesta(
        tabla=_texto(cruda, "tableName"),
        id_grupo=_texto(cruda, "groupId"),
        id_control=control,
        ids_miembros=miembros,
    )


def _variables_texto(correo: Any) -> dict[str, str]:
    if not _existe(correo, ("stringVars",)):
        return {}
    crudas = _buscar(correo, ("stringVars",))
    if not isinstance(crudas, Mapping):
        raise ErrorConfiguracion("NTF-CFG-001", "email.stringVars debe ser un objeto")
    return {str(clave): str(valor) for clave, valor in crudas.items()}


def _ruta_plantilla(correo: Any) -> str | None:
    if not _existe(correo, ("templatePath",)):
        return None
    return _texto(correo, "templatePath").strip()


def leer_configuracion_caso(datos: Any, caso_uso_esperado: str) -> ConfiguracionCaso:
    declarado = datos.get("useCaseId") if isinstance(datos, Mapping) else None
    if declarado != caso_uso_esperado:
        raise ErrorConfiguracion(
            "NTF-CFG-002",
            f"El useCaseId del JSON no coincide. JSON={declarado} ARG={caso_uso_esperado}",
        )
    correo = _buscar(datos, ("email",))
    return ConfiguracionCaso(
        caso_uso=declarado,
        plantilla_asunto=_texto(correo, "subject"),
        variables_texto=_variables_texto(correo),
        destinatarios=normalizar_destinatarios(_lista(correo, ("destinations",), "NTF-CFG-003")),
        consultas_resumen=_consultas(datos, TIPO_CONSULTA_RESUMEN),
        consultas_totales=_consultas(datos, TIPO_CONSULTA_TOTALES),
        consultas_presencia=_presencia(correo),
        compuesta=_compuesta(datos),
        ruta_plantilla=_ruta_plantilla(correo),
    )
