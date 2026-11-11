from typing import Any, Mapping

from notificacion_certificacion.dominio.constantes import (
    CATALOGO_POR_DEFECTO,
    ESTADO_ENVIADO_POR_DEFECTO,
    MIEMBRO_CONTROL_POR_DEFECTO,
    TIPO_CONSULTA_RESUMEN,
    TIPO_CONSULTA_TOTALES,
)
from notificacion_certificacion.dominio.destinatarios import normalizar_destinatarios
from notificacion_certificacion.dominio.errores import ErrorConfiguracion
from notificacion_certificacion.dominio.modelos import (
    ConfiguracionCaso,
    ConfiguracionCompuesta,
    ConfiguracionGlobal,
    Consulta,
)


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


def _texto_opcional(datos: Any, por_defecto: str, *ruta: str) -> str:
    if not _existe(datos, ruta):
        return por_defecto
    return _texto(datos, *ruta)


def _lista(datos: Any, ruta: tuple[str, ...], codigo: str) -> list[Any]:
    if not _existe(datos, ruta):
        return []
    valor = _buscar(datos, ruta)
    if not isinstance(valor, list):
        raise ErrorConfiguracion(codigo, f"La clave debe ser una lista: {'.'.join(ruta)}")
    return valor


def leer_configuracion_global(datos: Any) -> ConfiguracionGlobal:
    return ConfiguracionGlobal(
        bucket_activos=_texto(datos, "buckets", "assets"),
        tabla_notificaciones=_texto(datos, "dynamodb", "notification_table"),
        catalogo=_texto_opcional(datos, CATALOGO_POR_DEFECTO, "catalog"),
        ambiente=_texto(datos, "environment"),
        prefijo_salida_athena=_texto(datos, "athena", "output_path"),
        estado_enviado=_texto_opcional(datos, ESTADO_ENVIADO_POR_DEFECTO, "certification", "sent_status"),
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
    control = _texto_opcional(cruda, MIEMBRO_CONTROL_POR_DEFECTO, "controlMemberId")
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
