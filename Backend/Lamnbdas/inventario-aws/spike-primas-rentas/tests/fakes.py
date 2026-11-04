"""Dobles en memoria de los puertos y un CNF de ejemplo para las pruebas."""
import copy
import json


def cnf_ejemplo():
    def ciclo(n):
        return {
            "PK": "MALLA#PRIMAS_RENTAS",
            "SK": f"CICLO#{n:03d}",
            "orden": n,
            "nombre": f"ciclo {n}",
            "segmentos": [
                {
                    "orden": 1,
                    "nombre": "seg",
                    "forma": "UNICO",
                    "estructuras": [
                        {
                            "orden": 2,
                            "estructuraRef": f"EST#B{n}",
                            "tipoTabla": "DETALLE",
                            "where": "PERIODO = '202609' AND FEC_APROBACION_PENSION <= DATE '2026-09-30'",
                            "segmentacionHorizontal": False,
                        },
                        {
                            "orden": 1,
                            "estructuraRef": f"EST#A{n}",
                            "tipoTabla": "MAESTRA",
                            "segmentacionHorizontal": False,
                        },
                    ],
                }
            ],
        }

    def reg(sk, **campos):
        return {"PK": "MALLA#PRIMAS_RENTAS", "SK": sk, **campos}

    cnf = {
        "GENERAL": reg("GENERAL", nombreCaso="primas_rentas"),
        "EXTRACCION": reg(
            "EXTRACCION",
            fuente={"tipo": "oracle"},
            rutas={"bronze": "s3://b/rentas/202609/"},
            controlExtraccion={"periodo_negocio": "202609", "ruta": "s3://b/rentas/202609/p.json"},
            salidaRuntime={"x": 1},
            archivoActivoUri="s3://assets/rentas/config_extraccion.json",
        ),
        "VALIDACION": reg(
            "VALIDACION",
            parametrosExtraccion={"a": 1},
            salida={"b": 2},
            datasets=[],
            archivoActivoUri="s3://assets/rentas/config_val.json",
        ),
        "REMEDIACION": reg(
            "REMEDIACION",
            plantillaUri="s3://assets/rentas/plantilla_rem.json",
            estadoDefinicion="PROVISIONAL_SIN_REGLAS_TRANSFORMATIVAS",
            archivoActivoUri="s3://assets/rentas/config_rem.json",
        ),
        "SCD": reg(
            "SCD",
            estructuraGoldRef="EST#GOLD",
            salida={"c": 3},
            llaves=["k"],
            columnasCambio=["x"],
            columnasFijas=["y"],
            columnasAdministrativas=["z"],
            metadatos={"m": 1},
            archivoActivoUri="s3://assets/rentas/config_scd.json",
        ),
    }
    for n in range(1, 5):
        cnf[f"CICLO#{n:03d}"] = ciclo(n)
    return cnf


class FakeCnf:
    def __init__(self, cnf):
        self.cnf = cnf
        self.persistido = None

    def consultar(self, pk):
        return copy.deepcopy(self.cnf)

    def persistir_periodo(self, cnf, antes):
        self.persistido = (cnf, antes)


class FakePrs:
    def __init__(self, existentes=None):
        self.existentes = existentes

    def obtener(self, referencia, sk):
        if self.existentes is not None and (referencia, sk) not in self.existentes:
            return None
        if sk == "GLD":
            return {"PK": referencia, "SK": sk}
        return {"schema": "SCH", "tabla": f"Tabla_{referencia[-2:]}"}


class FakeAlmacen:
    def __init__(self, plantilla=None):
        self.plantilla = plantilla or {"nombre_caso": "primas_rentas", "reglas": []}
        self.escrito = []

    def leer(self, uri):
        return copy.deepcopy(self.plantilla)

    def escribir(self, uri, contenido, metadatos):
        self.escrito.append((uri, json.loads(json.dumps(contenido)), metadatos))
        return {"uri": uri, "etag": "e", "versionId": None}


class FakeOrquestador:
    def __init__(self):
        self.llamadas = []

    def iniciar(self, nombre, entrada):
        self.llamadas.append((nombre, entrada))
        return f"arn:exec:{nombre}"


class FakeEjecuciones:
    def __init__(self):
        self.items = []

    def registrar(self, item):
        self.items.append(item)
