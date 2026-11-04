"""Caso de uso: configurar una malla (validar, publicar y, opcionalmente, ejecutar)."""
import json
from datetime import datetime, timezone

from . import constructores, logs, periodo, validaciones
from .mallas import obtener_malla
from .modelos import Evento, Resultado


class Configurador:
    def __init__(self, ajustes, cnf, prs, almacen, orquestador, ejecuciones, reloj=None):
        self._ajustes = ajustes
        self._cnf = cnf
        self._prs = prs
        self._almacen = almacen
        self._orquestador = orquestador
        self._ejecuciones = ejecuciones
        self._reloj = reloj or (lambda: datetime.now(timezone.utc))

    def ejecutar(self, evento: Evento, request_id: str) -> Resultado:
        spec = obtener_malla(evento.malla or self._ajustes.malla_por_defecto)
        logs.registrar(
            "inicio", malla=spec.nombre, publicar=evento.publicar, ejecutar=evento.ejecutar
        )

        cnf = self._cnf.consultar(spec.pk)
        validaciones.validar_registros_cnf(cnf, spec)
        cnf, cnf_antes = periodo.preparar_periodo(cnf, evento.periodo_negocio)

        generados = self._generar(cnf, spec)
        validaciones.validar_generados(generados)

        resultado = Resultado(
            malla=spec.pk,
            publicar=evento.publicar,
            ejecutar=evento.ejecutar,
            periodo_negocio=generados["EXTRACCION"]["periodo_negocio"],
            registros_cnf=len(cnf),
        )
        if evento.publicar:
            resultado.publicados = self._publicar(cnf, cnf_antes, generados, spec)
        if evento.ejecutar:
            self._iniciar_ejecucion(evento, spec, cnf, resultado, request_id)

        logs.registrar("fin", **resultado.to_dict())
        return resultado

    def _generar(self, cnf, spec):
        return {
            "EXTRACCION": constructores.construir_extraccion(cnf, self._prs, spec),
            "VALIDACION": constructores.construir_validacion(cnf),
            "REMEDIACION": constructores.construir_remediacion(cnf, self._almacen, spec),
            "SCD": constructores.construir_scd(cnf, self._prs),
        }

    def _publicar(self, cnf, cnf_antes, generados, spec):
        self._cnf.persistir_periodo(cnf, cnf_antes)
        metadatos = {"origen": "dynamodb-prs-cnf", "malla": spec.slug}
        return [
            self._almacen.escribir(cnf[nombre]["archivoActivoUri"], contenido, metadatos)
            for nombre, contenido in generados.items()
        ]

    def _iniciar_ejecucion(self, evento, spec, cnf, resultado, request_id):
        ahora = self._reloj()
        marca = ahora.strftime("%Y%m%dT%H%M%SZ")
        nombre = f"{spec.prefijo_ejecucion}-{resultado.periodo_negocio}-auto-{marca}-{request_id[:8]}"
        ejc_pk = f"EJC#{spec.nombre}#{resultado.periodo_negocio}"
        ejc_sk = f"EJECUCION#{nombre}"

        arn = self._orquestador.iniciar(
            nombre,
            {
                "ejcPK": ejc_pk,
                "ejcSK": ejc_sk,
                "malla": spec.nombre,
                "periodoNegocio": resultado.periodo_negocio,
                "modoEjecucion": evento.modo_ejecucion,
                "rutaConfigExtraccion": cnf["EXTRACCION"]["archivoActivoUri"],
                "origen": evento.origen,
            },
        )
        self._ejecuciones.registrar(
            {
                "PK": ejc_pk,
                "SK": ejc_sk,
                "malla": spec.nombre,
                "periodoNegocio": resultado.periodo_negocio,
                "executionName": nombre,
                "executionArn": arn,
                "estado": "INICIADA",
                "fechaInicio": ahora.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "origen": evento.origen,
            }
        )
        resultado.execution_name, resultado.execution_arn = nombre, arn
        resultado.ejc_pk, resultado.ejc_sk = ejc_pk, ejc_sk
