from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

from notificacion_certificacion.aplicacion.compuesto import ServicioCompuesto
from notificacion_certificacion.aplicacion.consultas import ServicioConsultas
from notificacion_certificacion.aplicacion.enlaces import ServicioEnlaces
from notificacion_certificacion.aplicacion.puertos import (
    Bitacora,
    EventoLog,
    Notificador,
    Reloj,
    RepositorioConfiguracion,
    RepositorioNotificaciones,
)
from notificacion_certificacion.dominio.configuracion import (
    leer_configuracion_caso,
    leer_configuracion_global,
)
from notificacion_certificacion.dominio.constantes import (
    ACCION_CERTIFICAR,
    ACCION_DESCARGAR,
    ACCION_NO_CERTIFICAR,
    CANAL_CORREO,
    PREFIJO_CONSULTA_DETALLE,
)
from notificacion_certificacion.dominio.correo_html import (
    fragmentos_html,
    plantilla_admite_avisos,
    rellenar_plantilla,
)
from notificacion_certificacion.dominio.errores import ErrorNotificacion, ErrorPlataforma
from notificacion_certificacion.dominio.modelos import (
    Aviso,
    ConfiguracionCaso,
    ConfiguracionGlobal,
    EntornoConsultas,
    EnvioRealizado,
    Mensaje,
    ParametrosEjecucion,
    RegistroNotificacion,
)
from notificacion_certificacion.dominio.parametrizacion import limpiar_url
from notificacion_certificacion.dominio.redaccion import (
    aviso_por_registros,
    avisos_del_correo,
    componer_texto,
    construir_asunto,
    construir_cuerpo,
    formatear_archivos,
    nombre_de_archivo,
    variables_base,
    variables_finales,
)
from notificacion_certificacion.dominio.rutas import normalizar_clave, ruta_salida_consultas

SEPARADOR_IDENTIFICADORES = ","
ETIQUETA_CASO_USO = "useCaseId"
ETIQUETA_TIPO_CORREO = "emailType"
VARIABLE_ENLACE_CERTIFICAR = "certificarLink"
VARIABLE_ENLACE_NO_CERTIFICAR = "nocertificarLink"
VARIABLE_NOMBRE_CASO = "useCaseName"


class ResultadoJob(str, Enum):
    SIN_REGISTROS = "SIN_REGISTROS"
    YA_ENVIADO = "YA_ENVIADO"
    YA_COMPLETADO = "YA_COMPLETADO"
    ENVIADO = "ENVIADO"


@dataclass(frozen=True)
class Dependencias:
    configuracion: RepositorioConfiguracion
    notificaciones: RepositorioNotificaciones
    notificador: Notificador
    consultas: ServicioConsultas
    enlaces: ServicioEnlaces
    compuesto: ServicioCompuesto
    reloj: Reloj
    bitacora: Bitacora


class NotificarCertificacion:
    def __init__(self, parametros: ParametrosEjecucion, dependencias: Dependencias) -> None:
        self._parametros = parametros
        self._dependencias = dependencias

    def ejecutar(self) -> ResultadoJob:
        bitacora = self._dependencias.bitacora
        bitacora.registrar(
            EventoLog.INICIO_JOB, caso_uso=self._parametros.caso_uso, cert_code=self._parametros.cert_code
        )
        configuracion_global, configuracion_caso = self._cargar_configuraciones()
        registros = self._dependencias.notificaciones.buscar_por_cert_code(
            configuracion_global.tabla_notificaciones,
            self._parametros.indice_cert_code,
            self._parametros.cert_code,
            self._parametros.caso_uso,
        )
        omision = self._omision(registros, configuracion_global)
        if omision is not None:
            return omision
        resultado = self._notificar(configuracion_global, configuracion_caso, registros)
        bitacora.registrar(EventoLog.FIN_JOB, resultado=resultado.value)
        return resultado

    def _cargar_configuraciones(self) -> tuple[ConfiguracionGlobal, ConfiguracionCaso]:
        parametros = self._parametros
        repositorio = self._dependencias.configuracion
        configuracion_global = leer_configuracion_global(
            repositorio.cargar_json(parametros.bucket_configuracion, parametros.clave_global)
        )
        configuracion_caso = leer_configuracion_caso(
            repositorio.cargar_json(parametros.bucket_configuracion, parametros.clave_caso),
            parametros.caso_uso,
        )
        self._dependencias.bitacora.registrar(EventoLog.CONFIGURACION_CARGADA)
        return configuracion_global, configuracion_caso

    def _omision(
        self,
        registros: Sequence[RegistroNotificacion],
        configuracion_global: ConfiguracionGlobal,
    ) -> ResultadoJob | None:
        bitacora = self._dependencias.bitacora
        if not registros:
            bitacora.registrar(EventoLog.SIN_REGISTROS, nivel="WARN", cert_code=self._parametros.cert_code)
            return ResultadoJob.SIN_REGISTROS
        if all(registro.estado == configuracion_global.estado_enviado for registro in registros):
            bitacora.registrar(EventoLog.YA_ENVIADO, cert_code=self._parametros.cert_code)
            return ResultadoJob.YA_ENVIADO
        return None

    def _notificar(
        self,
        configuracion_global: ConfiguracionGlobal,
        configuracion_caso: ConfiguracionCaso,
        registros: Sequence[RegistroNotificacion],
    ) -> ResultadoJob:
        parametros = self._parametros
        reserva = self._dependencias.compuesto.preparar(
            configuracion_caso.compuesta,
            registros[0].periodo,
            parametros.cert_code,
            parametros.regeneracion_sin_datos_nuevos,
        )
        if reserva is not None and reserva.ya_completada:
            self._dependencias.bitacora.registrar(EventoLog.COMPUESTO_YA_COMPLETADO, cert_code=parametros.cert_code)
            return ResultadoJob.YA_COMPLETADO
        mensaje = self._armar_mensaje(configuracion_global, configuracion_caso, registros)
        envios = self._enviar(mensaje, configuracion_caso.destinatarios)
        self._marcar_enviados(configuracion_global, registros, envios)
        identificadores = SEPARADOR_IDENTIFICADORES.join(envio.id_mensaje for envio in envios)
        self._dependencias.compuesto.cerrar(reserva, identificadores)
        return ResultadoJob.ENVIADO

    def _armar_mensaje(
        self,
        configuracion_global: ConfiguracionGlobal,
        configuracion_caso: ConfiguracionCaso,
        registros: Sequence[RegistroNotificacion],
    ) -> Mensaje:
        parametros = self._parametros
        consultas = self._dependencias.consultas
        primero = registros[0]
        base = variables_base(configuracion_global.ambiente, configuracion_caso, primero, parametros.cert_code)
        finales = variables_finales(base, configuracion_caso.variables_texto)
        entorno = self._entorno(configuracion_global)
        resumen = consultas.tablas(configuracion_caso.consultas_resumen, base, entorno)
        totales = consultas.tablas(configuracion_caso.consultas_totales, base, entorno)
        aviso_miembros = consultas.disponibilidad(configuracion_caso.consultas_presencia, base, entorno)
        certificar = self._enlace(ACCION_CERTIFICAR, finales.get(VARIABLE_ENLACE_CERTIFICAR, ""))
        no_certificar = self._enlace(ACCION_NO_CERTIFICAR, finales.get(VARIABLE_ENLACE_NO_CERTIFICAR, ""))
        archivos = self._archivos(registros)
        cuerpo = construir_cuerpo(
            finales.get(VARIABLE_NOMBRE_CASO, parametros.caso_uso),
            resumen.texto,
            totales.texto,
            formatear_archivos(archivos),
            certificar,
            no_certificar,
        )
        aviso = aviso_miembros if aviso_miembros is not None else aviso_por_registros(registros, primero.periodo)
        regenerado = parametros.regeneracion_sin_datos_nuevos
        fragmentos = fragmentos_html(resumen.html, totales.html, archivos, certificar, no_certificar)
        return Mensaje(
            asunto=construir_asunto(configuracion_caso.plantilla_asunto, finales, regenerado),
            texto=componer_texto(cuerpo, aviso, regenerado),
            html=self._html(
                configuracion_global, configuracion_caso, finales, fragmentos, avisos_del_correo(aviso, regenerado)
            ),
            etiquetas={ETIQUETA_CASO_USO: parametros.caso_uso, ETIQUETA_TIPO_CORREO: parametros.tipo_correo},
        )

    @staticmethod
    def _entorno(configuracion_global: ConfiguracionGlobal) -> EntornoConsultas:
        return EntornoConsultas(
            catalogo=configuracion_global.catalogo,
            salida=ruta_salida_consultas(
                configuracion_global.bucket_activos, configuracion_global.prefijo_salida_athena
            ),
        )

    def _html(
        self,
        configuracion_global: ConfiguracionGlobal,
        configuracion_caso: ConfiguracionCaso,
        variables: Mapping[str, str],
        fragmentos: Mapping[str, str],
        avisos: Sequence[Aviso],
    ) -> str | None:
        ruta = configuracion_caso.ruta_plantilla
        if not ruta:
            return None
        bucket = configuracion_global.bucket_activos
        normalizar_clave(bucket, ruta)
        bitacora = self._dependencias.bitacora
        try:
            plantilla = self._dependencias.configuracion.cargar_texto(bucket, ruta)
        except ErrorNotificacion as error:
            bitacora.registrar(EventoLog.PLANTILLA_NO_LEIDA, nivel="WARN", ruta=ruta, **error.como_diccionario())
            return None
        if not plantilla_admite_avisos(plantilla, avisos):
            bitacora.registrar(EventoLog.PLANTILLA_SIN_MARCADOR, nivel="WARN", ruta=ruta)
            return None
        return rellenar_plantilla(plantilla, variables, fragmentos, avisos)

    def _enlace(self, accion: str, url: str) -> str:
        return self._dependencias.enlaces.crear(accion, limpiar_url(url))

    def _archivos(self, registros: Sequence[RegistroNotificacion]) -> list[tuple[str, str]]:
        detalles = sorted(
            (registro for registro in registros if registro.nombre_consulta.startswith(PREFIJO_CONSULTA_DETALLE)),
            key=lambda registro: registro.nombre_consulta,
        )
        return [(nombre_de_archivo(registro), self._enlace(ACCION_DESCARGAR, registro.url_s3)) for registro in detalles]

    def _enviar(self, mensaje: Mensaje, destinatarios: Sequence[str]) -> tuple[EnvioRealizado, ...]:
        bitacora = self._dependencias.bitacora
        total = len(destinatarios)
        envios: list[EnvioRealizado] = []
        ultimo_error: ErrorNotificacion | None = None
        for posicion, destinatario in enumerate(destinatarios, start=1):
            try:
                id_mensaje = self._enviar_uno(mensaje, destinatario)
            except ErrorNotificacion as error:
                ultimo_error = error
                bitacora.registrar(
                    EventoLog.ENVIO_FALLIDO, nivel="ERROR", posicion=posicion, total=total, **error.como_diccionario()
                )
                continue
            envios.append(EnvioRealizado(destinatario=destinatario, id_mensaje=id_mensaje))
            bitacora.registrar(EventoLog.CORREO_ENVIADO, posicion=posicion, total=total, id_mensaje=id_mensaje)
        if not envios and ultimo_error is not None:
            raise ultimo_error
        if len(envios) < total:
            bitacora.registrar(
                EventoLog.ENVIO_PARCIAL, nivel="WARN", enviados=len(envios), fallidos=total - len(envios)
            )
        return tuple(envios)

    def _enviar_uno(self, mensaje: Mensaje, destinatario: str) -> str:
        id_mensaje = self._dependencias.notificador.enviar(mensaje, destinatario)
        if not id_mensaje:
            raise ErrorPlataforma("NTF-PLA-003", "El servicio de correo no confirmo el identificador del mensaje")
        return id_mensaje

    def _marcar_enviados(
        self,
        configuracion_global: ConfiguracionGlobal,
        registros: Sequence[RegistroNotificacion],
        envios: Sequence[EnvioRealizado],
    ) -> None:
        self._dependencias.notificaciones.marcar_enviados(
            configuracion_global.tabla_notificaciones,
            registros,
            configuracion_global.estado_enviado,
            CANAL_CORREO,
            envios,
            self._dependencias.reloj.ahora_local().isoformat(),
        )
        self._dependencias.bitacora.registrar(EventoLog.REGISTROS_MARCADOS, cantidad=len(registros))
