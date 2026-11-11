import copy

import pytest

from constructores import CONFIG_GLOBAL, config_compuesta, copiar_config_caso
from notificacion_certificacion.dominio.configuracion import (
    leer_configuracion_caso,
    leer_configuracion_global,
)
from notificacion_certificacion.dominio.errores import ErrorConfiguracion


def _global(**cambios):
    datos = copy.deepcopy(CONFIG_GLOBAL)
    datos.update(cambios)
    return datos


def test_configuracion_global_se_lee_completa():
    configuracion = leer_configuracion_global(CONFIG_GLOBAL)
    assert configuracion.bucket_activos == "bucket-activos"
    assert configuracion.tabla_notificaciones == "tabla-notificaciones"
    assert configuracion.ambiente == "dev"
    assert configuracion.prefijo_salida_athena == "athena/salida"
    assert configuracion.estado_enviado == "SENT"
    assert configuracion.catalogo == "AwsDataCatalog"


def test_configuracion_global_respeta_catalogo_y_estado_configurados():
    datos = _global(catalog="otro_catalogo")
    datos["certification"] = {"sent_status": "ENVIADO"}
    configuracion = leer_configuracion_global(datos)
    assert configuracion.catalogo == "otro_catalogo"
    assert configuracion.estado_enviado == "ENVIADO"


def test_configuracion_global_usa_estado_por_defecto_si_no_hay_seccion():
    datos = _global()
    del datos["certification"]
    assert leer_configuracion_global(datos).estado_enviado == "SENT"


@pytest.mark.parametrize("clave", ["buckets", "dynamodb", "environment", "athena"])
def test_configuracion_global_exige_las_claves_obligatorias(clave):
    datos = _global()
    del datos[clave]
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_global(datos)
    assert error.value.codigo == "NTF-CFG-001"
    assert clave in error.value.mensaje


def test_configuracion_global_rechaza_valores_que_no_son_texto():
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_global(_global(environment="  "))
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_global(_global(environment=5))


def test_configuracion_global_rechaza_un_documento_que_no_es_objeto():
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_global([])


def test_configuracion_caso_normaliza_destinatarios_y_ordena_consultas():
    caso = leer_configuracion_caso(copiar_config_caso(), "casoA")
    assert caso.destinatarios == ("uno@example.com", "dos@example.com")
    assert [consulta.nombre for consulta in caso.consultas_resumen] == ["a_resumen", "b_resumen"]
    assert [consulta.nombre for consulta in caso.consultas_totales] == ["total"]
    assert caso.consultas_presencia == ()
    assert caso.compuesta is None
    assert caso.plantilla_asunto.startswith("NIIF 17")


def test_configuracion_caso_rechaza_un_useCaseId_distinto():
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(copiar_config_caso(), "otro")
    assert error.value.codigo == "NTF-CFG-002"


def test_configuracion_caso_rechaza_documentos_que_no_son_objeto():
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso([], "casoA")
    assert error.value.codigo == "NTF-CFG-002"


def test_configuracion_caso_exige_correo_y_asunto():
    sin_correo = copiar_config_caso()
    del sin_correo["email"]
    sin_asunto = copiar_config_caso()
    del sin_asunto["email"]["subject"]
    for datos in (sin_correo, sin_asunto):
        with pytest.raises(ErrorConfiguracion) as error:
            leer_configuracion_caso(datos, "casoA")
        assert error.value.codigo == "NTF-CFG-001"


def test_configuracion_caso_sin_destinatarios_falla_al_leer():
    datos = copiar_config_caso()
    datos["email"]["destinations"] = []
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(datos, "casoA")
    assert error.value.codigo == "NTF-CFG-003"


def test_configuracion_caso_sin_clave_destinatarios_falla():
    datos = copiar_config_caso()
    del datos["email"]["destinations"]
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(datos, "casoA")
    assert error.value.codigo == "NTF-CFG-003"


def test_configuracion_caso_exige_que_los_destinatarios_sean_lista():
    datos = copiar_config_caso()
    datos["email"]["destinations"] = "a@x.com"
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(datos, "casoA")
    assert error.value.codigo == "NTF-CFG-003"


def test_configuracion_caso_acepta_variables_de_texto_ausentes():
    datos = copiar_config_caso()
    del datos["email"]["stringVars"]
    assert leer_configuracion_caso(datos, "casoA").variables_texto == {}


def test_configuracion_caso_exige_que_las_variables_de_texto_sean_objeto():
    datos = copiar_config_caso()
    datos["email"]["stringVars"] = ["x"]
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_caso(datos, "casoA")


def test_configuracion_caso_sin_consultas_las_deja_vacias():
    datos = copiar_config_caso()
    del datos["queries"]
    caso = leer_configuracion_caso(datos, "casoA")
    assert caso.consultas_resumen == ()
    assert caso.consultas_totales == ()


def test_configuracion_caso_valida_la_forma_de_las_consultas():
    datos = copiar_config_caso()
    datos["queries"]["summary"] = ["no es objeto"]
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_caso(datos, "casoA")
    datos["queries"]["summary"] = {"no": "lista"}
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_caso(datos, "casoA")
    datos["queries"]["summary"] = [{"queryName": "x", "database": "d"}]
    with pytest.raises(ErrorConfiguracion):
        leer_configuracion_caso(datos, "casoA")


def test_configuracion_caso_lee_la_presencia_de_datos_por_miembro():
    datos = copiar_config_caso()
    datos["email"]["dataPresence"] = [
        {"name": "A", "database": "d", "query": "q1"},
        {"name": "B", "database": "d", "query": "q2"},
    ]
    caso = leer_configuracion_caso(datos, "casoA")
    assert [consulta.nombre for consulta in caso.consultas_presencia] == ["A", "B"]


@pytest.mark.parametrize(
    "presencia",
    [
        "no lista",
        [{"database": "d", "query": "q"}],
        [{"name": " ", "database": "d", "query": "q"}],
        ["texto"],
        [{"name": "A", "database": "d", "query": "q"}, {"name": "A", "database": "d", "query": "q"}],
    ],
)
def test_configuracion_caso_rechaza_presencia_invalida(presencia):
    datos = copiar_config_caso()
    datos["email"]["dataPresence"] = presencia
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(datos, "casoA")
    assert error.value.codigo == "NTF-CFG-005"


def test_configuracion_caso_lee_el_compuesto_con_control_por_defecto():
    datos = copiar_config_caso()
    datos["compositeNotification"] = config_compuesta()
    compuesta = leer_configuracion_caso(datos, "casoA").compuesta
    assert compuesta.tabla == "tabla-compuesto"
    assert compuesta.id_grupo == "grupo"
    assert compuesta.id_control == "_CONTROL"
    assert compuesta.ids_miembros == ("m1", "m2")


def test_configuracion_caso_respeta_el_miembro_de_control_configurado():
    datos = copiar_config_caso()
    datos["compositeNotification"] = {**config_compuesta(), "controlMemberId": "control"}
    assert leer_configuracion_caso(datos, "casoA").compuesta.id_control == "control"


def test_configuracion_caso_ignora_el_compuesto_deshabilitado():
    datos = copiar_config_caso()
    datos["compositeNotification"] = {**config_compuesta(), "enabled": False}
    assert leer_configuracion_caso(datos, "casoA").compuesta is None


@pytest.mark.parametrize(
    "cambios",
    [
        {"memberIds": []},
        {"memberIds": ["m1", "m1"]},
        {"memberIds": ["m1", "_CONTROL"]},
        {"memberIds": "m1"},
    ],
)
def test_configuracion_caso_rechaza_compuestos_invalidos(cambios):
    datos = copiar_config_caso()
    datos["compositeNotification"] = {**config_compuesta(), **cambios}
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(datos, "casoA")
    assert error.value.codigo == "NTF-CFG-004"


def test_configuracion_caso_exige_tabla_y_grupo_del_compuesto():
    datos = copiar_config_caso()
    compuesto = config_compuesta()
    del compuesto["tableName"]
    datos["compositeNotification"] = compuesto
    with pytest.raises(ErrorConfiguracion) as error:
        leer_configuracion_caso(datos, "casoA")
    assert error.value.codigo == "NTF-CFG-001"
