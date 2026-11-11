from datetime import timedelta

from notificacion_certificacion.aplicacion.puertos import (
    Bitacora,
    EventoLog,
    GeneradorTokens,
    Reloj,
    RepositorioEnlaces,
)
from notificacion_certificacion.dominio.modelos import EnlaceCorto, PoliticaEnlaces


class ServicioEnlaces:
    def __init__(
        self,
        repositorio: RepositorioEnlaces,
        reloj: Reloj,
        generador: GeneradorTokens,
        bitacora: Bitacora,
        politica: PoliticaEnlaces,
        caso_uso: str,
        cert_code: str,
    ) -> None:
        self._repositorio = repositorio
        self._reloj = reloj
        self._generador = generador
        self._bitacora = bitacora
        self._politica = politica
        self._caso_uso = caso_uso
        self._cert_code = cert_code

    def crear(self, accion: str, url_larga: str) -> str:
        ahora = self._reloj.ahora_local()
        vigencia = ahora + timedelta(days=self._politica.dias_vigencia)
        enlace = EnlaceCorto(
            token=self._generador.generar(self._politica.longitud_token),
            url=url_larga,
            caso_uso=self._caso_uso,
            cert_code=self._cert_code,
            accion=accion,
            fecha_creacion=ahora.isoformat(),
            expira_en=int(vigencia.timestamp()),
        )
        self._repositorio.guardar(self._politica.tabla, enlace)
        self._bitacora.registrar(EventoLog.ENLACE_CREADO, accion=accion, token=enlace.token)
        return f"{self._politica.url_base}?t={enlace.token}"
