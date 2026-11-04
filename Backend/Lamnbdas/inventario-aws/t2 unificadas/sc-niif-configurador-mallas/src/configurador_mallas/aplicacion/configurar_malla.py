"""Caso de uso: configurar una malla (validar, publicar y, si se pide, ejecutar).

Es el mismo para las seis mallas: lo que cambia entre ellas llega en ``MallaSpec``
desde el catálogo.
"""
from datetime import datetime, timezone

from ..dominio import constructores, periodo, validaciones
from ..dominio.errores import ErrorConfiguracion, ErrorConfigurador, ErrorEntrada, ErrorPlataforma
from .idempotencia import ejecucion_id as calcular_ejecucion_id
from ..dominio.modelos import PREFIJO_EJECUCION, Resultado
from .puertos import EventoLog, Metrica

ESTADO_EJECUCION_INICIADA = "INICIADA"
ORIGEN_METADATOS_S3 = "dynamodb-prs-cnf"


class ConfiguradorMallas:
    def __init__(self, *, catalogo, cnf, prs, programaciones, almacen, orquestador,
                 ejecuciones, idempotencia, reloj):
        self._catalogo = catalogo
        self._cnf = cnf
        self._prs = prs
        self._programaciones = programaciones
        self._almacen = almacen
        self._orquestador = orquestador
        self._ejecuciones = ejecuciones
        self._idempotencia = idempotencia
        self._reloj = reloj

    def ejecutar(self, evento, invocacion, observador):
        spec = self._catalogo.obtener(evento.malla)
        if evento.modo_ejecucion not in self._catalogo.modos_ejecucion:
            raise ErrorEntrada("modoEjecucion no permitido", codigo="CFG-ENT-005",
                               detalle={"permitidos": list(self._catalogo.modos_ejecucion)})

        clave = evento.clave_idempotencia or invocacion.request_id
        id_corrida = calcular_ejecucion_id(spec.codigo, clave)
        observador.con_contexto(malla=spec.codigo, ejecucion_id=id_corrida)
        observador.evento(EventoLog.CORRIDA_INICIADA, publicar=evento.publicar,
                          ejecutar=evento.ejecutar, origen=evento.origen,
                          modoEjecucion=evento.modo_ejecucion, claveIdempotencia=clave)

        previo = self._idempotencia.iniciar(spec.codigo, clave, invocacion.tiempo_restante_ms)
        if previo is not None:
            observador.evento(EventoLog.IDEMPOTENCIA_REUTILIZADA,
                              executionArn=previo.execution_arn)
            observador.metrica(Metrica.CORRIDA_REUTILIZADA, 1, "Count")
            return previo

        try:
            resultado = self._procesar(spec, evento, id_corrida, observador)
        except ErrorConfigurador as error:
            self._idempotencia.fallar(spec.codigo, clave, error)
            raise
        except Exception as exc:
            error = ErrorPlataforma("Error no clasificado", codigo="CFG-PLT-000",
                                    detalle={"tipo": type(exc).__name__})
            self._idempotencia.fallar(spec.codigo, clave, error)
            raise error from exc

        self._idempotencia.completar(spec.codigo, clave, resultado)
        observador.evento(EventoLog.CORRIDA_FINALIZADA, periodoNegocio=resultado.periodo_negocio,
                          publicados=len(resultado.publicados),
                          executionArn=resultado.execution_arn)
        observador.metrica(Metrica.CORRIDA_EXITOSA, 1, "Count")
        return resultado

    def _procesar(self, spec, evento, id_corrida, observador):
        periodo_negocio = self._resolver_periodo(spec, evento)
        observador.con_contexto(periodo_negocio=periodo_negocio)

        cnf = self._cnf.consultar(spec.pk)
        validaciones.validar_registros_cnf(cnf, spec)
        cnf, antes = periodo.preparar_periodo(cnf, periodo_negocio, spec.reglas_periodo)

        generados = constructores.construir_todos(cnf, self._prs, self._almacen, spec)
        generados["EXTRACCION"] = periodo.resolver_estructura(
            generados["EXTRACCION"], periodo_negocio, periodo_negocio)
        validaciones.validar_generados(generados, spec)
        observador.evento(EventoLog.CONFIGURACION_LEIDA, registrosCNF=len(cnf))

        resultado = Resultado(
            malla=spec.pk,
            publicar=evento.publicar,
            ejecutar=evento.ejecutar,
            periodo_negocio=periodo_negocio,
            registros_cnf=len(cnf),
            ejecucion_id=id_corrida,
        )
        if evento.publicar:
            resultado.publicados = self._publicar(spec, cnf, antes, generados, id_corrida)
            observador.evento(EventoLog.CONFIGURACION_PUBLICADA,
                              archivos=[p["uri"] for p in resultado.publicados])
        if evento.ejecutar:
            self._iniciar_ejecucion(spec, evento, cnf, resultado, observador)
        return resultado

    def _resolver_periodo(self, spec, evento):
        """El periodo lo fija la programación (PRG); el evento solo puede confirmarlo."""
        programacion = self._programaciones.obtener(spec.prg_pk, spec.prg_sk)
        if not programacion:
            raise ErrorConfiguracion("No existe el registro de programación de la malla",
                                     codigo="CFG-CNF-006",
                                     detalle={"pk": spec.prg_pk, "sk": spec.prg_sk})
        try:
            periodo_prg = periodo.validar_formato(programacion.get("periodoNegocio"))
        except ErrorEntrada:
            raise ErrorConfiguracion("PRG.periodoNegocio no tiene formato YYYYMM",
                                     codigo="CFG-CNF-006") from None
        if evento.periodo_negocio is not None and evento.periodo_negocio != periodo_prg:
            raise ErrorEntrada("El periodo del evento difiere del de la programación",
                               codigo="CFG-ENT-003",
                               detalle={"evento": evento.periodo_negocio, "prg": periodo_prg})
        return periodo_prg

    def _publicar(self, spec, cnf, antes, generados, id_corrida):
        self._cnf.persistir_periodo(cnf, antes)
        metadatos = {"origen": ORIGEN_METADATOS_S3, "malla": spec.slug,
                     "ejecucion-id": id_corrida}
        return [
            self._almacen.escribir(cnf[nombre]["archivoActivoUri"], contenido, metadatos)
            for nombre, contenido in generados.items()
        ]

    def _iniciar_ejecucion(self, spec, evento, cnf, resultado, observador):
        periodo_negocio = resultado.periodo_negocio
        nombre = f"{spec.prefijo_ejecucion}-{periodo_negocio}-auto-{resultado.ejecucion_id}"
        ejc_pk = spec.ejc_pk(periodo_negocio)
        ejc_sk = f"{PREFIJO_EJECUCION}{nombre}"
        entrada = {
            "ejcPK": ejc_pk,
            "ejcSK": ejc_sk,
            "malla": spec.codigo,
            "periodoNegocio": periodo_negocio,
            "modoEjecucion": evento.modo_ejecucion,
            "rutaConfigExtraccion": cnf["EXTRACCION"]["archivoActivoUri"],
            "origen": evento.origen,
            "ejecucionId": resultado.ejecucion_id,
        }
        if spec.incluir_rango_fechas:
            inicio, _, fin = periodo.rango_mes(periodo_negocio)
            entrada["fecDesde"] = inicio.isoformat()
            entrada["fecHasta"] = fin.isoformat()

        arn = self._orquestador.iniciar(spec.state_machine, nombre, entrada)
        registrado = self._ejecuciones.registrar({
            "PK": ejc_pk,
            "SK": ejc_sk,
            "malla": spec.codigo,
            "periodoNegocio": periodo_negocio,
            "executionName": nombre,
            "executionArn": arn,
            "estado": ESTADO_EJECUCION_INICIADA,
            "fechaInicio": datetime.fromtimestamp(self._reloj(), timezone.utc)
                                   .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "origen": evento.origen,
            "ejecucionId": resultado.ejecucion_id,
        })
        observador.evento(EventoLog.EJECUCION_INICIADA if registrado
                          else EventoLog.EJECUCION_YA_REGISTRADA,
                          executionName=nombre, executionArn=arn)
        resultado.execution_name, resultado.execution_arn = nombre, arn
        resultado.ejc_pk, resultado.ejc_sk = ejc_pk, ejc_sk
