from datetime import datetime, timezone

import pytest

from configurador.config import Ajustes
from configurador.servicio import Configurador

from fakes import FakeAlmacen, FakeCnf, FakeEjecuciones, FakeOrquestador, FakePrs, cnf_ejemplo


@pytest.fixture
def ajustes():
    return Ajustes(
        region="us-east-1",
        cnf_table="cnf",
        prs_table="prs",
        ejc_table="ejc",
        state_machine_arn="arn:sm",
        malla_por_defecto="PRIMAS_RENTAS",
    )


@pytest.fixture
def mundo(ajustes):
    """Servicio armado con dobles; devuelve (servicio, dobles)."""

    class Mundo:
        cnf = FakeCnf(cnf_ejemplo())
        prs = FakePrs()
        almacen = FakeAlmacen()
        orquestador = FakeOrquestador()
        ejecuciones = FakeEjecuciones()

    m = Mundo()
    m.servicio = Configurador(
        ajustes,
        m.cnf,
        m.prs,
        m.almacen,
        m.orquestador,
        m.ejecuciones,
        reloj=lambda: datetime(2026, 11, 3, 10, 0, 0, tzinfo=timezone.utc),
    )
    return m
