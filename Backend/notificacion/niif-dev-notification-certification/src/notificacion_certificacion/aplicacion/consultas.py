from typing import Mapping, Sequence

from notificacion_certificacion.aplicacion.puertos import Bitacora, ConsultorAnalitico, EventoLog
from notificacion_certificacion.dominio.disponibilidad import extraer_cantidad
from notificacion_certificacion.dominio.errores import ErrorConsultaFallida
from notificacion_certificacion.dominio.modelos import Consulta, EntornoConsultas, ResultadoConsulta
from notificacion_certificacion.dominio.parametrizacion import parametrizar
from notificacion_certificacion.dominio.redaccion import aviso_disponibilidad
from notificacion_certificacion.dominio.tablas import formatear_resultado, unir_bloques
from notificacion_certificacion.dominio.textos import TEXTO_ERROR_CONSULTA


class ServicioConsultas:
    def __init__(self, consultor: ConsultorAnalitico, bitacora: Bitacora) -> None:
        self._consultor = consultor
        self._bitacora = bitacora

    def tabla(
        self,
        consultas: Sequence[Consulta],
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> str:
        return unir_bloques(self._texto(consulta, variables, entorno) for consulta in consultas)

    def disponibilidad(
        self,
        consultas: Sequence[Consulta],
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> str | None:
        if not consultas:
            return None
        vacios = [
            consulta.nombre
            for consulta in consultas
            if self._cantidad(consulta, variables, entorno) == 0
        ]
        return aviso_disponibilidad(str(variables["period"]), vacios, len(consultas))

    def _ejecutar(
        self,
        consulta: Consulta,
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> ResultadoConsulta:
        base_datos = parametrizar(consulta.base_datos, variables, desescapar=True)
        sql = parametrizar(consulta.sql, variables, desescapar=True)
        resultado = self._consultor.ejecutar(sql, base_datos, entorno)
        self._bitacora.registrar(
            EventoLog.CONSULTA_EJECUTADA, consulta=consulta.nombre, filas=len(resultado.filas)
        )
        return resultado

    def _texto(
        self,
        consulta: Consulta,
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> str:
        try:
            return formatear_resultado(self._ejecutar(consulta, variables, entorno))
        except ErrorConsultaFallida as error:
            self._bitacora.registrar(
                EventoLog.CONSULTA_FALLIDA, nivel="ERROR", consulta=consulta.nombre, **error.como_diccionario()
            )
            return TEXTO_ERROR_CONSULTA

    def _cantidad(
        self,
        consulta: Consulta,
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> int:
        return extraer_cantidad(self._ejecutar(consulta, variables, entorno), consulta.nombre)
