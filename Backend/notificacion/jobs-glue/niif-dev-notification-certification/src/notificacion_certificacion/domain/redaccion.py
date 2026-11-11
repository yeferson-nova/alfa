from string import Template
from typing import Mapping, Sequence

from notificacion_certificacion.domain.correo_html import TONO_ADVERTENCIA, TONO_INFORMATIVO
from notificacion_certificacion.domain.modelos import Aviso, ConfiguracionCaso, RegistroNotificacion
from notificacion_certificacion.domain.parametrizacion import parametrizar
from notificacion_certificacion.domain.periodo import rango_periodo
from notificacion_certificacion.domain.textos import (
    AVISO_DEMAS_MIEMBROS,
    AVISO_NINGUN_MIEMBRO,
    AVISO_REGENERACION,
    AVISO_SIN_DATOS_PARCIAL,
    AVISO_SIN_DATOS_TOTAL,
    ENCABEZADO_DISPONIBILIDAD,
    LINEA_SIN_DATOS_MIEMBRO,
    PLANTILLA_CUERPO,
    PREFIJO_ASUNTO_REGENERADO,
    TEXTO_ARCHIVO_POR_DEFECTO,
    TEXTO_SIN_ARCHIVOS,
    TEXTO_SIN_RESUMEN,
    TEXTO_SIN_TOTALES,
)

SEPARADOR_AVISO = "\n\n"


def variables_base(
    ambiente: str,
    caso: ConfiguracionCaso,
    registro: RegistroNotificacion,
    cert_code: str,
) -> dict[str, str]:
    return {
        "env": ambiente,
        "period": registro.periodo,
        "periodRange": rango_periodo(registro.periodo),
        "useCaseId": caso.caso_uso,
        "useCaseName": caso.variables_texto.get("useCaseName", caso.caso_uso),
        "certCode": cert_code,
        "userEmail": registro.correo_usuario,
        "executionId": registro.id_ejecucion,
    }


def variables_finales(base: Mapping[str, str], variables_texto: Mapping[str, str]) -> dict[str, str]:
    resueltas = {clave: parametrizar(valor, base) for clave, valor in variables_texto.items()}
    return {**base, **resueltas}


def construir_asunto(plantilla: str, variables: Mapping[str, str], regenerado: bool) -> str:
    asunto = Template(plantilla).safe_substitute(variables)
    asunto = asunto.replace("\n", " ").replace("\r", " ")
    return PREFIJO_ASUNTO_REGENERADO + asunto if regenerado else asunto


def nombre_de_archivo(registro: RegistroNotificacion) -> str:
    return registro.nombre_archivo or TEXTO_ARCHIVO_POR_DEFECTO


def formatear_archivos(entradas: Sequence[tuple[str, str]]) -> str:
    if not entradas:
        return TEXTO_SIN_ARCHIVOS
    return "\n".join(f"- {nombre}\n  {enlace}" for nombre, enlace in entradas)


def construir_cuerpo(
    nombre_caso: str,
    resumen: str,
    totales: str,
    archivos: str,
    certificar: str,
    no_certificar: str,
) -> str:
    return PLANTILLA_CUERPO.format(
        caso=nombre_caso,
        resumen=resumen or TEXTO_SIN_RESUMEN,
        totales=totales or TEXTO_SIN_TOTALES,
        archivos=archivos,
        certificar=certificar,
        no_certificar=no_certificar,
    )


def aviso_por_registros(registros: Sequence[RegistroNotificacion], periodo: str) -> str:
    vacios = [registro for registro in registros if registro.sin_datos_periodo]
    if not vacios:
        return ""
    if len(vacios) == len(registros):
        return AVISO_SIN_DATOS_TOTAL.format(periodo=periodo)
    consultas = ", ".join(registro.nombre_consulta for registro in vacios)
    return AVISO_SIN_DATOS_PARCIAL.format(periodo=periodo, consultas=consultas)


def aviso_disponibilidad(periodo: str, vacios: Sequence[str], total_miembros: int) -> str:
    if not vacios:
        return ""
    lineas = [ENCABEZADO_DISPONIBILIDAD]
    lineas.extend(LINEA_SIN_DATOS_MIEMBRO.format(nombre=nombre, periodo=periodo) for nombre in vacios)
    lineas.append(AVISO_NINGUN_MIEMBRO if len(vacios) == total_miembros else AVISO_DEMAS_MIEMBROS)
    return "\n".join(lineas)


def componer_texto(cuerpo: str, aviso: str, regenerado: bool) -> str:
    texto = cuerpo
    if aviso:
        texto = aviso + SEPARADOR_AVISO + texto
    if regenerado:
        texto = AVISO_REGENERACION + SEPARADOR_AVISO + texto
    return texto


def avisos_del_correo(aviso: str, regenerado: bool) -> list[Aviso]:
    avisos = [Aviso(TONO_ADVERTENCIA, aviso)] if aviso else []
    if regenerado:
        avisos.insert(0, Aviso(TONO_INFORMATIVO, AVISO_REGENERACION))
    return avisos
