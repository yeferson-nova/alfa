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
    archivos_a_html,
    componer_html,
    escapar,
    plantilla_admite_avisos,
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
    TablaCorreo,
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
from notificacion_certificacion.dominio.rutas import ruta_salida_consultas

SEPARADOR_IDENTIFICADORES = ","
ETIQUETA_CASO_USO = "useCaseId"
ETIQUETA_TIPO_CORREO = "emailType"
VARIABLE_ENLACE_CERTIFICAR = "certificarLink"
VARIABLE_ENLACE_NO_CERTIFICAR = "nocertificarLink"
VARIABLE_NOMBRE_CASO = "useCaseName"
FRAGMENTO_TOTALES = "tableTotal"
FRAGMENTO_RESUMEN = "tableSummary"
FRAGMENTO_DETALLE = "tableDetail"
FORMATO_HTML = "HTML"
FORMATO_TEXTO = "TEXTO"


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


@dataclass(frozen=True)
class Contenido:
    nombre_caso: str
    resumen: TablaCorreo
    totales: TablaCorreo
    archivos: list[tuple[str, str]]
    certificar: str
    no_certificar: str
    aviso: str
    avisos: list[Aviso]


class NotificarCertificacion:
    def __init__(self, parametros: ParametrosEjecucion, dependencias: Dependencias) -> None:
        self._parametros = parametros
        self._dependencias = dependencias
        self.enviados = 0
        self.fallidos = 0

    def ejecutar(self) -> ResultadoJob:
        bitacora = self._dependencias.bitacora
        bitacora.fijar_contexto(
            ejecucion_id=self._parametros.id_ejecucion,
            request_id=self._parametros.id_corrida,
            cert_code=self._parametros.cert_code,
            use_case_id=self._parametros.caso_uso,
        )
        bitacora.registrar(EventoLog.INICIO_JOB)
        configuracion_global, configuracion_caso = self._cargar_configuraciones()
        registros = self._dependencias.notificaciones.buscar_por_cert_code(
            configuracion_global.tabla_notificaciones,
            self._parametros.indice_cert_code,
            self._parametros.cert_code,
            self._parametros.caso_uso,
        )
        self._fijar_contexto_de_registros(registros)
        omision = self._omision(registros, configuracion_global)
        resultado = omision or self._notificar(configuracion_global, configuracion_caso, registros)
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

    def _fijar_contexto_de_registros(self, registros: Sequence[RegistroNotificacion]) -> None:
        bitacora = self._dependencias.bitacora
        if not registros:
            return
        bitacora.fijar_contexto(periodo_negocio=registros[0].periodo)
        recibido = self._parametros.id_ejecucion
        del_registro = next((registro.ejecucion_flujo for registro in registros if registro.ejecucion_flujo), "")
        if recibido and del_registro and recibido != del_registro:
            bitacora.registrar(EventoLog.ID_FLUJO_DISTINTO, nivel="WARN", ejecucion_registro=del_registro)
        if not recibido and del_registro:
            bitacora.fijar_contexto(ejecucion_id=del_registro)
        if not recibido and not del_registro:
            bitacora.registrar(EventoLog.ID_FLUJO_AUSENTE, nivel="WARN")

    def _omision(
        self,
        registros: Sequence[RegistroNotificacion],
        configuracion_global: ConfiguracionGlobal,
    ) -> ResultadoJob | None:
        bitacora = self._dependencias.bitacora
        if not registros:
            bitacora.registrar(EventoLog.SIN_REGISTROS, nivel="WARN")
            return ResultadoJob.SIN_REGISTROS
        if all(registro.estado == configuracion_global.estado_enviado for registro in registros):
            bitacora.registrar(EventoLog.YA_ENVIADO)
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
            self._dependencias.bitacora.registrar(EventoLog.COMPUESTO_YA_COMPLETADO)
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
        base = variables_base(
            configuracion_global.ambiente, configuracion_caso, registros[0], self._parametros.cert_code
        )
        finales = variables_finales(base, configuracion_caso.variables_texto)
        contenido = self._contenido(configuracion_global, configuracion_caso, registros, base, finales)
        regenerado = self._parametros.regeneracion_sin_datos_nuevos
        return Mensaje(
            asunto=construir_asunto(configuracion_caso.plantilla_asunto, finales, regenerado),
            texto=componer_texto(self._cuerpo(contenido), contenido.aviso, regenerado),
            html=self._html(configuracion_global, configuracion_caso, finales, contenido),
            etiquetas={
                ETIQUETA_CASO_USO: self._parametros.caso_uso,
                ETIQUETA_TIPO_CORREO: self._parametros.tipo_correo,
            },
        )

    def _contenido(
        self,
        configuracion_global: ConfiguracionGlobal,
        configuracion_caso: ConfiguracionCaso,
        registros: Sequence[RegistroNotificacion],
        base: Mapping[str, str],
        finales: Mapping[str, str],
    ) -> Contenido:
        consultas = self._dependencias.consultas
        entorno = EntornoConsultas(
            catalogo=configuracion_global.catalogo,
            salida=ruta_salida_consultas(
                configuracion_global.bucket_activos, configuracion_global.prefijo_salida_athena
            ),
        )
        resumen = consultas.tabla(configuracion_caso.consultas_resumen, base, entorno)
        totales = consultas.tabla(configuracion_caso.consultas_totales, base, entorno)
        aviso_miembros = consultas.disponibilidad(configuracion_caso.consultas_presencia, base, entorno)
        aviso = aviso_miembros if aviso_miembros is not None else aviso_por_registros(registros, registros[0].periodo)
        avisos = avisos_del_correo(aviso, self._parametros.regeneracion_sin_datos_nuevos)
        if avisos:
            self._dependencias.bitacora.registrar(
                EventoLog.AVISOS_AGREGADOS, tonos=[item.tono for item in avisos], cantidad=len(avisos)
            )
        return Contenido(
            nombre_caso=finales.get(VARIABLE_NOMBRE_CASO, self._parametros.caso_uso),
            resumen=resumen,
            totales=totales,
            archivos=self._archivos(registros),
            certificar=self._enlace(ACCION_CERTIFICAR, finales.get(VARIABLE_ENLACE_CERTIFICAR, "")),
            no_certificar=self._enlace(ACCION_NO_CERTIFICAR, finales.get(VARIABLE_ENLACE_NO_CERTIFICAR, "")),
            aviso=aviso,
            avisos=avisos,
        )

    @staticmethod
    def _cuerpo(contenido: Contenido) -> str:
        return construir_cuerpo(
            contenido.nombre_caso,
            contenido.resumen.texto,
            contenido.totales.texto,
            formatear_archivos(contenido.archivos),
            contenido.certificar,
            contenido.no_certificar,
        )

    def _html(
        self,
        configuracion_global: ConfiguracionGlobal,
        configuracion_caso: ConfiguracionCaso,
        variables: Mapping[str, str],
        contenido: Contenido,
    ) -> str | None:
        plantilla = self._plantilla(configuracion_global, configuracion_caso, contenido.avisos)
        formato = FORMATO_TEXTO if plantilla is None else FORMATO_HTML
        self._dependencias.bitacora.registrar(EventoLog.FORMATO_CORREO, formato=formato)
        if plantilla is None:
            return None
        fragmentos = {
            FRAGMENTO_TOTALES: contenido.totales.html,
            FRAGMENTO_RESUMEN: contenido.resumen.html,
            FRAGMENTO_DETALLE: archivos_a_html(contenido.archivos),
            VARIABLE_ENLACE_CERTIFICAR: escapar(contenido.certificar),
            VARIABLE_ENLACE_NO_CERTIFICAR: escapar(contenido.no_certificar),
        }
        return componer_html(plantilla, variables, fragmentos, contenido.avisos)

    def _plantilla(
        self,
        configuracion_global: ConfiguracionGlobal,
        configuracion_caso: ConfiguracionCaso,
        avisos: Sequence[Aviso],
    ) -> str | None:
        ruta = configuracion_caso.ruta_plantilla
        if not ruta:
            return None
        bitacora = self._dependencias.bitacora
        try:
            plantilla = self._dependencias.configuracion.cargar_texto(configuracion_global.bucket_activos, ruta)
        except ErrorNotificacion as error:
            bitacora.registrar(EventoLog.PLANTILLA_NO_DISPONIBLE, nivel="WARN", ruta=ruta, **error.como_diccionario())
            return None
        if not plantilla_admite_avisos(plantilla, avisos):
            bitacora.registrar(EventoLog.PLANTILLA_NO_DISPONIBLE, nivel="WARN", ruta=ruta, motivo="SIN_MARCADOR_AVISOS")
            return None
        return plantilla

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
        envios = []
        for posicion, destinatario in enumerate(destinatarios, start=1):
            id_mensaje = self._enviar_uno(mensaje, destinatario, posicion, len(destinatarios))
            if id_mensaje:
                envios.append(EnvioRealizado(destinatario=destinatario, id_mensaje=id_mensaje))
        self.enviados, self.fallidos = len(envios), len(destinatarios) - len(envios)
        if not envios:
            raise ErrorPlataforma("NTF-PLA-006", "SES no envio el correo a ningun destinatario")
        if self.fallidos:
            bitacora.registrar(EventoLog.ENVIO_PARCIAL, nivel="WARN", enviados=self.enviados, fallidos=self.fallidos)
        return tuple(envios)

    def _enviar_uno(self, mensaje: Mensaje, destinatario: str, posicion: int, total: int) -> str:
        bitacora = self._dependencias.bitacora
        try:
            id_mensaje = self._dependencias.notificador.enviar(mensaje, destinatario)
            if not id_mensaje:
                raise ErrorPlataforma("NTF-PLA-003", "El servicio de correo no confirmo el identificador del mensaje")
        except ErrorNotificacion as error:
            bitacora.registrar(
                EventoLog.CORREO_FALLIDO, nivel="ERROR", posicion=posicion, total=total, **error.como_diccionario()
            )
            return ""
        bitacora.registrar(EventoLog.CORREO_ENVIADO, posicion=posicion, total=total, id_mensaje=id_mensaje)
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
