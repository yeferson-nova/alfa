"""Dobles en memoria de los puertos. Imitan el comportamiento de los adaptadores."""
import copy

from configurador_mallas.dominio.errores import ErrorConfiguracion, ErrorOrquestacion
from configurador_mallas.aplicacion.idempotencia import COMPLETADO, EN_PROCESO, FALLIDO


class FakeCnf:
    def __init__(self, registros):
        self.registros = copy.deepcopy(registros)
        self.persistencias = []

    def consultar(self, pk):
        return copy.deepcopy({k: v for k, v in self.registros.items() if v["PK"] == pk})

    def persistir_periodo(self, cnf, antes):
        cambios = 0
        for clave, item in cnf.items():
            if item != antes[clave]:
                self.registros[clave] = copy.deepcopy(item)
                cambios += 1
        self.persistencias.append(cambios)
        return cambios


class FakeLectura:
    def __init__(self, datos):
        self.datos = copy.deepcopy(datos)

    def obtener(self, pk, sk):
        return copy.deepcopy(self.datos.get((pk, sk)))


class FakeAlmacen:
    def __init__(self, archivos):
        self.archivos = copy.deepcopy(archivos)
        self.escrituras = []

    def leer(self, uri):
        if uri not in self.archivos:
            raise ErrorConfiguracion("Recurso no encontrado", codigo="CFG-CNF-009")
        return copy.deepcopy(self.archivos[uri])

    def escribir(self, uri, contenido, metadatos):
        self.archivos[uri] = copy.deepcopy(contenido)
        self.escrituras.append((uri, metadatos))
        return {"uri": uri, "etag": "etag", "versionId": None}


class FakeOrquestador:
    def __init__(self):
        self.ejecuciones = {}
        self.llamadas = 0

    def iniciar(self, maquina, nombre, entrada):
        self.llamadas += 1
        if nombre in self.ejecuciones and self.ejecuciones[nombre] != (maquina, entrada):
            raise ErrorOrquestacion("Ya existe", codigo="CFG-ORQ-001")
        self.ejecuciones[nombre] = (maquina, copy.deepcopy(entrada))
        return f"arn:aws:states:us-east-1:111111111111:execution:{maquina}:{nombre}"


class FakeEjecuciones:
    def __init__(self):
        self.items = {}

    def registrar(self, item):
        llave = (item["PK"], item["SK"])
        if llave in self.items:
            return False
        self.items[llave] = copy.deepcopy(item)
        return True


class FakeIdempotencia:
    """Misma condición que el adaptador: libre, fallido o con bloqueo vencido."""

    def __init__(self):
        self.registros = {}

    def reclamar(self, ambito, clave, ahora, bloqueo_hasta, expira_en):
        actual = self.registros.get((ambito, clave))
        libre = (actual is None or actual["estado"] == FALLIDO
                 or (actual["estado"] == EN_PROCESO and actual["bloqueoHasta"] < ahora))
        if not libre:
            return copy.deepcopy(actual)
        self.registros[(ambito, clave)] = {"estado": EN_PROCESO, "bloqueoHasta": bloqueo_hasta,
                                           "expiraEn": expira_en}
        return None

    def guardar(self, ambito, clave, estado, expira_en, resultado=None, error=None):
        registro = self.registros.setdefault((ambito, clave), {})
        registro.update({"estado": estado, "expiraEn": expira_en})
        if resultado is not None:
            registro["resultado"] = copy.deepcopy(resultado)
        if error is not None:
            registro["error"] = copy.deepcopy(error)

    def estado(self, ambito, clave):
        return self.registros[(ambito, clave)]["estado"]


__all__ = ["FakeCnf", "FakeLectura", "FakeAlmacen", "FakeOrquestador", "FakeEjecuciones",
           "FakeIdempotencia", "COMPLETADO"]
