import json
import runpy
import sys
from pathlib import Path

import pytest

from fakes import BitacoraEnMemoria, RelojFijo
from notificacion_certificacion.adapters.inbound import job_glue
from notificacion_certificacion.adapters.outbound.bitacora_json import BitacoraJson
from notificacion_certificacion.adapters.outbound.reloj import GeneradorTokensUuid, RelojSistema
from notificacion_certificacion.application.notificar_certificacion import ResultadoJob
from niif_observability import StructuredLogger

from notificacion_certificacion.application.puertos import MENSAJES_EVENTO, EventoLog
from notificacion_certificacion.config.ajustes import leer_parametros
from notificacion_certificacion.config.composicion import Contexto, construir_contexto
from notificacion_certificacion.domain.errores import ErrorConfiguracion, ErrorEntrada, ErrorPlataforma
from constructores import construir_parametros, copiar_config_global
from notificacion_certificacion.adapters.outbound.s3_configuracion import RepositorioConfiguracionS3

ARGUMENTOS = {
    "useCaseId": "casoA",
    "certCode": "CERT1",
    "emailType": "Certification",
    "CONFIG_BUCKET": "bucket-config",
    "CONFIG_KEY": "casos/casoA.json",
    "GLOBAL_CONFIG_KEY": "global.json",
}


def _argv(**cambios):
    valores = {**ARGUMENTOS, **cambios}
    argv = []
    for nombre, valor in valores.items():
        if valor is not None:
            argv.extend([f"--{nombre}", str(valor)])
    return argv


def _sin(*nombres):
    return [elemento for par in _pares(ARGUMENTOS, nombres) for elemento in par]


def _pares(valores, excluidos):
    return [(f"--{nombre}", valor) for nombre, valor in valores.items() if nombre not in excluidos]


def test_leer_parametros_construye_el_contrato_con_valores_normalizados():
    parametros = leer_parametros(_argv(**{"JOB_RUN_ID": "jr_1"}))

    assert parametros.caso_uso == "casoA"
    assert parametros.tipo_correo == "certification"
    assert parametros.clave_global == "global.json"
    assert parametros.regeneracion_sin_datos_nuevos is False
    assert parametros.id_corrida == "jr_1"
    assert parametros.id_ejecucion is None


def test_leer_parametros_toma_el_id_del_flujo_si_llega():
    parametros = leer_parametros(_argv(EJECUCION_ID=" arl-202606-auto-x-1 "))
    assert parametros.id_ejecucion == "arl-202606-auto-x-1"


def test_leer_parametros_no_inventa_identificadores_si_no_llegan():
    parametros = leer_parametros(_argv())
    assert (parametros.id_ejecucion, parametros.id_corrida) == (None, None)


def test_leer_parametros_acepta_la_marca_de_regeneracion():
    parametros = leer_parametros(_argv(REGENERACION_SIN_DATOS_NUEVOS="TRUE"))
    assert parametros.regeneracion_sin_datos_nuevos is True


@pytest.mark.parametrize(
    "retirado",
    [
        "REDIRECT_BASE_URL",
        "SHORT_LINKS_TABLE",
        "SES_SENDER",
        "SES_CONFIGURATION_SET",
        "INDICE_CERT_CODE",
        "ZONA_HORARIA",
        "DIAS_VIGENCIA_ENLACE",
        "LONGITUD_TOKEN_ENLACE",
        "SEGUNDOS_ENTRE_CONSULTAS",
        "SEGUNDOS_MAXIMOS_CONSULTA",
    ],
)
def test_leer_parametros_rechaza_los_argumentos_que_pasaron_al_json_global(retirado):
    with pytest.raises(ErrorEntrada) as error:
        leer_parametros(_argv(**{retirado: "valor"}))
    assert error.value.codigo == "NTF-ENT-004"
    assert retirado in error.value.mensaje


def test_leer_parametros_rechaza_un_argumento_retirado_escrito_con_igual():
    with pytest.raises(ErrorEntrada) as error:
        leer_parametros(_argv() + ["--ZONA_HORARIA=America/Bogota"])
    assert "timezone" in error.value.mensaje


def test_leer_parametros_ignora_los_argumentos_propios_de_glue():
    parametros = leer_parametros(_argv() + ["--JOB_NAME", "job", "--enable-metrics", "true"])
    assert parametros.caso_uso == "casoA"


def test_leer_parametros_no_acepta_abreviaturas_de_argumentos():
    argv = [token for token in _argv() if token != "--useCaseId" and token != "casoA"] + ["--useCase", "casoA"]
    with pytest.raises(ErrorEntrada):
        leer_parametros(argv)


@pytest.mark.parametrize("faltante", sorted(ARGUMENTOS))
def test_leer_parametros_exige_cada_argumento_obligatorio(faltante):
    with pytest.raises(ErrorEntrada) as error:
        leer_parametros(_sin(faltante))
    assert error.value.codigo == "NTF-ENT-001"


@pytest.mark.parametrize(
    "cambio",
    [
        {"useCaseId": "   "},
        {"GLOBAL_CONFIG_KEY": " "},
        {"REGENERACION_SIN_DATOS_NUEVOS": "quiza"},
    ],
)
def test_leer_parametros_rechaza_valores_invalidos(cambio):
    with pytest.raises(ErrorEntrada) as error:
        leer_parametros(_argv(**cambio))
    assert error.value.codigo == "NTF-ENT-001"


def test_bitacora_escribe_una_linea_json_por_evento_con_el_contexto_y_mensaje_en_espanol():
    lineas: list[str] = []
    bitacora = BitacoraJson(StructuredLogger("componente", lineas.append, RelojFijo().ahora_utc))
    bitacora.fijar_contexto(ejecucion_id="ejec-9", cert_code="CERT1")

    bitacora.registrar(EventoLog.CORREO_ENVIADO, nivel="WARN", posicion=1, id_mensaje="ñ")

    (linea,) = lineas
    datos = json.loads(linea)
    assert datos == {
        "marcaTiempo": "2026-11-04T15:00:00+00:00",
        "nivel": "WARN",
        "evento": "NTF_CORREO_ENVIADO",
        "mensaje": "Correo aceptado por SES para un destinatario",
        "funcion": "componente",
        "ejecucion_id": "ejec-9",
        "request_id": None,
        "cert_code": "CERT1",
        "use_case_id": None,
        "periodo_negocio": None,
        "posicion": 1,
        "id_mensaje": "ñ",
    }
    assert "ñ" in linea


def test_bitacora_guarda_el_mensaje_del_error_como_detalle():
    lineas: list[str] = []
    bitacora = BitacoraJson(StructuredLogger("componente", lineas.append))
    bitacora.registrar(EventoLog.ERROR_JOB, nivel="ERROR", **ErrorPlataforma("NTF-PLA-004", "rechazado").como_diccionario())
    datos = json.loads(lineas[0])
    assert (datos["mensaje"], datos["detalle"]) == ("El job falló con un error clasificado", "rechazado")


def test_todos_los_eventos_tienen_mensaje_en_espanol():
    assert set(MENSAJES_EVENTO) == set(EventoLog)


def test_reloj_sistema_entrega_hora_local_y_utc_con_zona():
    reloj = RelojSistema("America/Bogota")
    assert reloj.ahora_local().utcoffset().total_seconds() == -5 * 3600
    assert reloj.ahora_utc().utcoffset().total_seconds() == 0


def test_reloj_sistema_rechaza_zonas_inexistentes():
    with pytest.raises(ErrorConfiguracion) as error:
        RelojSistema("Mundo/Imaginario")
    assert error.value.codigo == "NTF-CFG-001"


def test_generador_de_tokens_respeta_la_longitud_y_no_repite():
    generador = GeneradorTokensUuid()
    tokens = {generador.generar(8) for _ in range(50)}
    assert len(tokens) == 50
    assert all(len(token) == 8 for token in tokens)


class _CasoDeUso:
    def __init__(self, resultado=None, error=None):
        self.resultado = resultado
        self.error = error
        self.enviados = 2
        self.fallidos = 1

    def ejecutar(self):
        if self.error is not None:
            raise self.error
        return self.resultado


def _fabricas(caso_de_uso, bitacora, argumentos_recibidos=None):
    def leer(argv):
        if argumentos_recibidos is not None:
            argumentos_recibidos.append(list(argv))
        return construir_parametros()

    def construir(_parametros):
        return Contexto(caso_de_uso=caso_de_uso, bitacora=bitacora, caso_uso="casoA")

    return leer, construir


def test_ejecutar_devuelve_el_resultado_y_emite_las_metricas():
    lineas: list[str] = []
    bitacora = BitacoraEnMemoria()
    leer, construir = _fabricas(_CasoDeUso(resultado=ResultadoJob.ENVIADO), bitacora)
    resultado = job_glue.ejecutar(["--x", "1"], leer=leer, construir=construir, escribir=lineas.append)

    assert resultado == ResultadoJob.ENVIADO
    evento, _, resumen = bitacora.eventos[-1]
    assert evento == EventoLog.RESUMEN_JOB
    assert (resumen["job_exitoso"], resumen["job_fallido"]) == (1, 0)
    assert (resumen["correos_enviados"], resumen["correos_fallidos"]) == (2, 1)
    assert resumen["duracion_ms"] >= 0
    assert lineas == []


def test_ejecutar_toma_los_argumentos_de_la_linea_de_comandos_si_no_se_indican(monkeypatch):
    recibidos: list[list[str]] = []
    monkeypatch.setattr(sys, "argv", ["job.py", "--a", "1"])
    leer, construir = _fabricas(_CasoDeUso(resultado=ResultadoJob.ENVIADO), BitacoraEnMemoria(), recibidos)

    job_glue.ejecutar(leer=leer, construir=construir, escribir=[].append)

    assert recibidos == [["--a", "1"]]


def test_ejecutar_registra_y_relanza_los_errores_clasificados():
    bitacora = BitacoraEnMemoria()
    error = ErrorConfiguracion("NTF-CFG-001", "falta clave")
    leer, construir = _fabricas(_CasoDeUso(error=error), bitacora)

    lineas: list[str] = []
    with pytest.raises(ErrorConfiguracion):
        job_glue.ejecutar([], leer=leer, construir=construir, escribir=lineas.append)

    assert bitacora.eventos[-1][2]["job_fallido"] == 1
    evento, nivel, contexto = bitacora.eventos[0]
    assert (evento, nivel) == (EventoLog.ERROR_JOB, "ERROR")
    assert contexto["codigo"] == "NTF-CFG-001"
    assert contexto["dominio"] == "CONFIGURACION"


def test_ejecutar_clasifica_y_relanza_los_errores_inesperados():
    bitacora = BitacoraEnMemoria()
    leer, construir = _fabricas(_CasoDeUso(error=RuntimeError("fallo inesperado")), bitacora)

    with pytest.raises(RuntimeError):
        job_glue.ejecutar([], leer=leer, construir=construir, escribir=[].append)

    _, _, contexto = bitacora.eventos[0]
    assert contexto["codigo"] == "NTF-PLA-999"
    assert contexto["mensaje"] == "fallo inesperado"


def test_ejecutar_registra_en_json_y_relanza_los_errores_de_arranque():
    def leer(_argv):
        raise ErrorEntrada("NTF-ENT-001", "falta argumento")

    lineas: list[str] = []
    with pytest.raises(ErrorEntrada):
        job_glue.ejecutar([], leer=leer, construir=lambda parametros: None, escribir=lineas.append)

    (linea,) = lineas
    datos = json.loads(linea)
    assert (datos["evento"], datos["nivel"], datos["codigo"]) == ("NTF_ERROR_JOB", "ERROR", "NTF-ENT-001")
    assert datos["detalle"] == "falta argumento"


def test_ejecutar_clasifica_los_errores_inesperados_de_arranque():
    def construir(_parametros):
        raise RuntimeError("sin red")

    lineas: list[str] = []
    with pytest.raises(RuntimeError):
        job_glue.ejecutar([], leer=lambda argv: construir_parametros(), construir=construir, escribir=lineas.append)

    assert json.loads(lineas[0])["codigo"] == "NTF-PLA-999"


def _global_en_s3(monkeypatch, **cambios):
    datos = copiar_config_global()
    datos.update(cambios)
    lecturas = []

    def cargar_json(_repositorio, bucket, clave):
        lecturas.append((bucket, clave))
        return datos

    monkeypatch.setattr(RepositorioConfiguracionS3, "cargar_json", cargar_json)
    return lecturas


def test_construir_contexto_lee_el_json_global_y_arma_el_caso_de_uso(monkeypatch):
    lecturas = _global_en_s3(monkeypatch)
    contexto = construir_contexto(construir_parametros())
    assert contexto.caso_de_uso is not None
    assert contexto.bitacora is not None
    assert lecturas == [("bucket-config", "global.json")]


def test_construir_contexto_rechaza_una_zona_horaria_invalida(monkeypatch):
    _global_en_s3(monkeypatch, timezone="Mundo/Imaginario")
    with pytest.raises(ErrorConfiguracion) as error:
        construir_contexto(construir_parametros())
    assert "timezone" in error.value.mensaje


def test_construir_contexto_falla_si_el_json_global_no_trae_la_seccion_de_notificacion(monkeypatch):
    datos = copiar_config_global()
    del datos["notification"]
    monkeypatch.setattr(RepositorioConfiguracionS3, "cargar_json", lambda *_: datos)
    with pytest.raises(ErrorConfiguracion) as error:
        construir_contexto(construir_parametros())
    assert error.value.codigo == "NTF-CFG-001"


def test_script_del_job_invoca_el_punto_de_entrada(monkeypatch):
    llamadas = []
    monkeypatch.setattr(job_glue, "ejecutar", lambda: llamadas.append("ejecutado"))
    script = Path(__file__).resolve().parents[1] / "src" / "job_notificacion.py"

    runpy.run_path(str(script), run_name="__main__")

    assert llamadas == ["ejecutado"]


def test_script_del_job_no_ejecuta_nada_al_importarse(monkeypatch):
    llamadas = []
    monkeypatch.setattr(job_glue, "ejecutar", lambda: llamadas.append("ejecutado"))
    script = Path(__file__).resolve().parents[1] / "src" / "job_notificacion.py"

    runpy.run_path(str(script), run_name="importado")

    assert llamadas == []


def test_errores_de_plataforma_se_pueden_construir_con_codigo_no_clasificado():
    assert ErrorPlataforma("NTF-PLA-999", "x").como_diccionario()["codigo"] == "NTF-PLA-999"
