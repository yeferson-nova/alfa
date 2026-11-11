from typing import Mapping, Sequence

from notificacion_certificacion.application.puertos import Bitacora, ConsultorAnalitico, EventoLog
from notificacion_certificacion.domain.correo_html import bloque_tabla, error_a_html, resultado_a_html
from notificacion_certificacion.domain.disponibilidad import extraer_cantidad
from notificacion_certificacion.domain.errores import ErrorConsultaFallida
from notificacion_certificacion.domain.modelos import Consulta, EntornoConsultas, ResultadoConsulta, TablaCorreo
from notificacion_certificacion.domain.parametrizacion import parametrizar_estricto
from notificacion_certificacion.domain.redaccion import aviso_disponibilidad
from notificacion_certificacion.domain.tablas import formatear_resultado, unir_bloques
from notificacion_certificacion.domain.textos import TEXTO_ERROR_CONSULTA


class ServicioConsultas:
    def __init__(self, consultor: ConsultorAnalitico, bitacora: Bitacora) -> None:
        self._consultor = consultor
        self._bitacora = bitacora

    def tabla(
        self,
        consultas: Sequence[Consulta],
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> TablaCorreo:
        bloques = [self._bloque(consulta, variables, entorno) for consulta in consultas]
        return TablaCorreo(
            texto=unir_bloques(texto for texto, _ in bloques),
            html="".join(bloque_tabla(html) for _, html in bloques),
        )

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
        base_datos = parametrizar_estricto(consulta.base_datos, variables, f"{consulta.nombre}.database")
        sql = parametrizar_estricto(consulta.sql, variables, f"{consulta.nombre}.query")
        resultado = self._consultor.ejecutar(sql, base_datos, entorno)
        self._bitacora.registrar(
            EventoLog.CONSULTA_EJECUTADA,
            consulta=consulta.nombre,
            filas=len(resultado.filas),
            id_consulta=resultado.id_consulta,
            bytes_escaneados=resultado.bytes_escaneados,
        )
        return resultado

    def _bloque(
        self,
        consulta: Consulta,
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> tuple[str, str]:
        try:
            resultado = self._ejecutar(consulta, variables, entorno)
        except ErrorConsultaFallida as error:
            self._bitacora.registrar(
                EventoLog.CONSULTA_FALLIDA, nivel="ERROR", consulta=consulta.nombre, **error.como_diccionario()
            )
            return TEXTO_ERROR_CONSULTA, error_a_html()
        return formatear_resultado(resultado), resultado_a_html(resultado)

    def _cantidad(
        self,
        consulta: Consulta,
        variables: Mapping[str, str],
        entorno: EntornoConsultas,
    ) -> int:
        return extraer_cantidad(self._ejecutar(consulta, variables, entorno), consulta.nombre)
