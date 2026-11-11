from notificacion_certificacion.application.puertos import Bitacora, EventoLog, Reloj, RepositorioCompuesto
from notificacion_certificacion.domain.compuesto import (
    control_ya_completado,
    identificador_compuesto,
    validar_miembro,
)
from notificacion_certificacion.domain.errores import ErrorPlataforma
from notificacion_certificacion.domain.modelos import (
    ConfiguracionCompuesta,
    RegistroCompuesto,
    ReservaCompuesta,
)


class ServicioCompuesto:
    def __init__(self, repositorio: RepositorioCompuesto, reloj: Reloj, bitacora: Bitacora) -> None:
        self._repositorio = repositorio
        self._reloj = reloj
        self._bitacora = bitacora

    def preparar(
        self,
        configuracion: ConfiguracionCompuesta | None,
        periodo: str,
        cert_code: str,
        regeneracion: bool,
    ) -> ReservaCompuesta | None:
        if regeneracion or configuracion is None:
            return None
        id_compuesto = identificador_compuesto(configuracion, periodo)
        control = self._repositorio.obtener(configuracion.tabla, id_compuesto, configuracion.id_control)
        completada = control_ya_completado(control, cert_code)
        miembros = () if completada else self._miembros(configuracion, id_compuesto, periodo)
        return ReservaCompuesta(
            tabla=configuracion.tabla,
            id_compuesto=id_compuesto,
            cert_code=cert_code,
            control=control,
            miembros=miembros,
            ya_completada=completada,
        )

    def cerrar(self, reserva: ReservaCompuesta | None, id_mensaje: str) -> None:
        if reserva is None or reserva.ya_completada:
            return
        if not id_mensaje:
            raise ErrorPlataforma("NTF-PLA-003", "El envio no devolvio identificador; no se consumen los miembros")
        self._repositorio.consumir(reserva, id_mensaje, self._reloj.ahora_utc().isoformat())
        self._bitacora.registrar(
            EventoLog.COMPUESTO_CERRADO, compuesto=reserva.id_compuesto, cert_code=reserva.cert_code
        )

    def _miembros(
        self,
        configuracion: ConfiguracionCompuesta,
        id_compuesto: str,
        periodo: str,
    ) -> tuple[RegistroCompuesto, ...]:
        return tuple(
            validar_miembro(
                self._repositorio.obtener(configuracion.tabla, id_compuesto, id_miembro), id_miembro, periodo
            )
            for id_miembro in configuracion.ids_miembros
        )
