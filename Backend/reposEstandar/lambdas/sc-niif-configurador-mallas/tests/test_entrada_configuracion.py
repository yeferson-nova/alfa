"""Adaptador de entrada (handler), observabilidad y raíz de composición."""
import json
from types import SimpleNamespace

import pytest

from configurador_mallas.adaptadores.entrada import lambda_handler as handler
from configurador_mallas.adaptadores.salida.cloudwatch import Observador
from configurador_mallas.aplicacion.puertos import Metrica
from configurador_mallas.configuracion import composicion
from configurador_mallas.dominio.errores import (ErrorConfiguracion, ErrorEntrada,
                                                 ErrorPlataforma)
from conftest import MALLA, Entorno

ENTORNO = {
    "AWS_REGION": "us-east-1", "CNF_TABLE": "cnf", "PRS_TABLE": "prs", "PRG_TABLE": "prg",
    "EJC_TABLE": "ejc", "IDEMPOTENCIA_TABLE": "idem", "IDEMPOTENCIA_TTL_SEGUNDOS": "3600",
    "CATALOGO_MALLAS_URI": "s3://assets/catalogo.json", "METRICAS_NAMESPACE": "NIIF/Configurador",
}
ARN = "arn:aws:lambda:us-east-1:111111111111:function:datalake-dev-niif-configurador-mallas-lambda"


def _contexto(request_id="req-1"):
    return SimpleNamespace(function_name="datalake-dev-niif-configurador-mallas-lambda",
                           aws_request_id=request_id, invoked_function_arn=ARN,
                           get_remaining_time_in_millis=lambda: 30_000)


@pytest.fixture
def lambda_lista(monkeypatch, cat):
    for nombre, valor in ENTORNO.items():
        monkeypatch.setenv(nombre, valor)
    monkeypatch.delenv("MALLA_PK", raising=False)
    monkeypatch.delenv("NIVEL_LOG", raising=False)
    handler.reiniciar()
    entorno = Entorno(cat)
    creaciones = []

    def fabrica(ajustes, contexto):
        creaciones.append(contexto.invoked_function_arn)
        return entorno.servicio

    monkeypatch.setattr(handler, "fabrica_servicio", fabrica)
    yield entorno, creaciones
    handler.reiniciar()


def _lineas(capsys):
    return [json.loads(linea) for linea in capsys.readouterr().out.splitlines()]


class TestHandler:
    def test_respuesta_y_logs_json_para_cloudwatch(self, lambda_lista, capsys):
        entorno, creaciones = lambda_lista
        respuesta = handler.lambda_handler({"malla": MALLA}, _contexto())
        assert respuesta["estado"] == "VALIDADO"
        assert respuesta["malla"] == f"MALLA#{MALLA}"
        lineas = _lineas(capsys)
        eventos = [l["evento"] for l in lineas if "evento" in l]
        assert eventos[0] == "CORRIDA_INICIADA" and eventos[-1] == "CORRIDA_FINALIZADA"
        assert all(l["request_id"] == "req-1" for l in lineas if "evento" in l)
        duracion = [l for l in lineas if "DuracionMs" in l][0]
        assert duracion["_aws"]["CloudWatchMetrics"][0]["Namespace"] == "NIIF/Configurador"
        assert creaciones == [ARN]

    def test_el_servicio_se_crea_una_vez_por_contenedor(self, lambda_lista):
        _, creaciones = lambda_lista
        handler.lambda_handler({"malla": MALLA}, _contexto("a"))
        handler.lambda_handler({"malla": MALLA}, _contexto("b"))
        assert len(creaciones) == 1

    def test_error_clasificado_se_registra_y_se_relanza(self, lambda_lista, capsys):
        with pytest.raises(ErrorEntrada):
            handler.lambda_handler({"malla": MALLA, "publicar": "si"}, _contexto())
        fallo = [l for l in _lineas(capsys) if l.get("evento") == "CORRIDA_FALLIDA"][0]
        assert fallo["nivel"] == "ERROR"
        assert fallo["error"]["codigo"] == "CFG-ENT-001"
        assert fallo["error"]["recuperable"] is False
        assert fallo["error"]["destinatario"] == "Equipo de desarrollo"

    def test_error_inesperado_se_clasifica_como_plataforma(self, lambda_lista, monkeypatch,
                                                           capsys):
        entorno, _ = lambda_lista
        monkeypatch.setattr(entorno.servicio, "ejecutar",
                            lambda *a: (_ for _ in ()).throw(KeyError("x")))
        with pytest.raises(ErrorPlataforma) as error:
            handler.lambda_handler({"malla": MALLA}, _contexto())
        assert error.value.detalle == {"tipo": "KeyError"}
        assert any(l.get("CorridaFallida") == 1 for l in _lineas(capsys))

    def test_falta_configuracion_del_entorno(self, lambda_lista, monkeypatch, capsys):
        monkeypatch.delenv("CNF_TABLE")
        monkeypatch.setenv("NIVEL_LOG", "NO_EXISTE")
        handler.reiniciar()
        with pytest.raises(ErrorConfiguracion):
            handler.lambda_handler({"malla": MALLA}, _contexto())
        lineas = _lineas(capsys)
        assert lineas[-1]["error"]["detalle"]["variables"] == ["CNF_TABLE"]
        assert not any("_aws" in l for l in lineas)  # sin namespace no se emiten métricas

    def test_malla_por_defecto_del_entorno(self, lambda_lista, monkeypatch):
        monkeypatch.setenv("MALLA_PK", f"MALLA#{MALLA}")
        handler.reiniciar()
        monkeypatch.setattr(handler, "fabrica_servicio", lambda a, c: lambda_lista[0].servicio)
        assert handler.lambda_handler({}, _contexto())["malla"] == f"MALLA#{MALLA}"


class TestObservador:
    def test_formato_emf(self, capturador):
        capturador.observador.con_contexto(malla="X1", ejecucion_id="abc", vacio=None)
        capturador.observador.metrica(Metrica.CORRIDA_EXITOSA, 1, "Count", Dominio="D")
        linea = capturador.lineas()[0]
        definicion = linea["_aws"]["CloudWatchMetrics"][0]
        assert definicion["Dimensions"] == [["Funcion", "Malla", "Dominio"]]
        assert definicion["Metrics"] == [{"Name": "CorridaExitosa", "Unit": "Count"}]
        assert (linea["Malla"], linea["CorridaExitosa"], linea["ejecucion_id"]) == ("X1", 1, "abc")
        assert "vacio" not in linea

    def test_mensajes_de_texto_tambien_salen_en_json(self, capturador):
        logger = capturador.observador._logger
        logger.warning("texto %s", "libre")
        assert capturador.lineas()[0] == {**capturador.lineas()[0], "mensaje": "texto libre",
                                          "nivel": "WARNING"}

    def test_sin_namespace_no_hay_metricas(self, capturador):
        observador = Observador(capturador.observador._logger, None, "f")
        observador.metrica(Metrica.DURACION, 5, "Milliseconds")
        assert capturador.lineas() == []


class TestComposicion:
    def test_partes_del_arn(self):
        assert composicion.partes_arn(ARN) == ("aws", "us-east-1", "111111111111")
        assert composicion.partes_arn(ARN + ":prod")[2] == "111111111111"

    @pytest.mark.parametrize("arn", [None, "", "arn:aws:s3:::bucket", "no-es-arn"])
    def test_arn_invalido(self, arn):
        with pytest.raises(ErrorConfiguracion):
            composicion.partes_arn(arn)

    def test_crear_servicio_conecta_los_adaptadores(self, monkeypatch, cat):
        from configurador_mallas.adaptadores.salida import s3
        from configurador_mallas.configuracion.ajustes import Ajustes
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prueba")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prueba")
        monkeypatch.setattr(s3, "cargar_catalogo", lambda almacen, uri: cat)
        servicio = composicion.crear_servicio(Ajustes.desde_entorno(dict(ENTORNO)), _contexto())
        assert servicio._catalogo is cat
        assert servicio._orquestador.arn_maquina("m").endswith(":111111111111:stateMachine:m")
