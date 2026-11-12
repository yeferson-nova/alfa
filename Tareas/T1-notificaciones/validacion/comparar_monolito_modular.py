import copy
import json
import sys
import types
from pathlib import Path
from unittest import mock

RAIZ = Path(sys.argv[1])
MONOLITO = RAIZ / "Tareas/paquete-release-notificaciones/niif-dev-notification-certification/notification_certification.py"
JOB = Path(sys.argv[2]) if len(sys.argv) > 2 else RAIZ / "Backend/ReposAws/glue/data-datalake-niif-notification-certification-glue-job"
CFG = RAIZ / "Backend/ReposAws/s3/cfg-datalake-niif-config-s3/src"
TPL = RAIZ / "Backend/ReposAws/s3/tpl-datalake-niif-email-template-s3/src"
sys.path[:0] = [str(JOB / "src"), str(JOB / "test")]

from fakes import InMemoryEventLogger, SequentialTokenGenerator, FixedClock, FakeCompositeRepository  # noqa: E402
from fakes import FakeConfigRepository, FakeLinkRepository, FakeNotificationRepository  # noqa: E402
from builders import build_parameters  # noqa: E402
from certification_notifier.application.composite import CompositeService  # noqa: E402
from certification_notifier.application.queries import QueryService  # noqa: E402
from certification_notifier.application.links import LinkService  # noqa: E402
from certification_notifier.application.notify_certification import Dependencies, NotifyCertification  # noqa: E402
from certification_notifier.domain.errors import QueryFailedError  # noqa: E402
from certification_notifier.domain.models import LinkPolicy, NotificationRecord, QueryResult  # noqa: E402

REDIRECCION = "https://redir.example/dev/r"
CERT = "CERT-EQ-1"
PERIODO = "202610"
CASOS = [
    "auxilios-funerarios/auxilioFunerario.json",
    "previsional/previsional.json",
    "primas-arl/primasArl.json",
    "primas-endosos/primasRentasEndosos.json",
    "primas-rentas/primasRentasEndosos.json",
    "reserva-matematica/reservaMatematicaRviArp.json",
]
ESCENARIOS = ["normal", "regenerado", "sin_datos_total", "sin_datos_parcial", "consulta_fallida", "miembro_vacio"]


def leer(ruta):
    return json.loads(ruta.read_text(encoding="utf-8").replace("{env}", "dev"))


def cargar_monolito():
    utils = types.ModuleType("awsglue.utils")
    utils.getResolvedOptions = lambda argv, nombres: {n: "x" for n in nombres}
    sys.modules["awsglue"] = types.ModuleType("awsglue")
    sys.modules["awsglue.utils"] = utils
    modulo = types.ModuleType("monolito")
    with mock.patch("boto3.client"), mock.patch("boto3.resource"):
        exec(compile(MONOLITO.read_text(encoding="utf-8"), str(MONOLITO), "exec"), modulo.__dict__)
    return modulo


def items_dynamo(caso_uso, escenario):
    base = [
        ("detail#b_det", "Detalle B & C.csv", "https://s3.example/b.csv?X-Amz=1&amp;y=2"),
        ("detail#a_det", "Detalle A.csv", "https://s3.example/a.csv?q=<1>"),
        ("summary#resumen", "", ""),
    ]
    items = []
    for posicion, (consulta, archivo, url) in enumerate(base):
        sin_datos = escenario == "sin_datos_total" or (escenario == "sin_datos_parcial" and posicion == 0)
        items.append({
            "useCaseId": caso_uso, "executionId": f"ex{posicion}", "period": PERIODO, "status": "PENDING",
            "userEmail": "u@example.com", "queryName": consulta, "nameFile": archivo, "s3_url": url,
            "sinDatosPeriodo": sin_datos,
        })
    return items


def respuesta(sql, presencias, escenario, primera_resumen):
    if sql in presencias:
        cero = escenario == "miembro_vacio" and sql == next(iter(presencias))
        return ("registros",), (("0" if cero else "7",),), False
    if escenario == "consulta_fallida" and sql == primera_resumen:
        return (), (), True
    return ("ramo", "valor <cop>"), ((f"R{len(sql) % 9}", "1.234,56 & más"), ("Z", "")), False


def correr_monolito(mono, global_cfg, caso_cfg, plantilla, escenario, presencias, primera):
    enviados, marcados, contador = [], [], [0]

    def athena(database, query, variables, salida, catalogo):
        sql = mono.parametrizar(query, variables, unescape=True)
        enc, filas, error = respuesta(sql, presencias, escenario, primera)
        return {"error": error, "headers": list(enc), "rows": [list(f) for f in filas]}

    def enlace(accion, url):
        contador[0] += 1
        return f"{REDIRECCION}?t={str(contador[0]).zfill(8)}"

    def enviar(destino, asunto, texto, html_cuerpo=None):
        enviados.append((destino, asunto, texto, html_cuerpo))
        return f"mid-{len(enviados)}"

    mono.useCaseId, mono.certCode, mono.emailType = caso_cfg["useCaseId"], CERT, "certification"
    mono.REGENERACION_SIN_DATOS_NUEVOS = escenario == "regenerado"
    mono.cargar_json = lambda b, k: global_cfg if k == "global" else caso_cfg
    mono.load_text = lambda b, k: plantilla
    mono.obtener_regs = lambda t: items_dynamo(caso_cfg["useCaseId"], escenario)
    mono.query_athena = athena
    mono.crear_short_link = enlace
    mono.send_ses = enviar
    mono.marcar_sent = lambda t, i, s, r, d: marcados.append((len(i), s, r["MessageId"], list(d)))
    mono.cerrar_compuesto_consumed = lambda *a: None
    mono.CONFIG_BUCKET, mono.CONFIG_KEY, mono.GLOBAL_CONFIG_KEY = "b", "caso", "global"
    mono.main()
    return enviados, marcados


class Consultor:
    def __init__(self, presencias, escenario, primera):
        self.datos = (presencias, escenario, primera)

    def run(self, sql, database, query_environment):
        enc, filas, error = respuesta(sql, *self.datos)
        if error:
            raise QueryFailedError("NTF-PLA-002", "fallo")
        return QueryResult(enc, filas)


class Notificador:
    def __init__(self):
        self.enviados = []

    def send(self, message, destination):
        self.enviados.append((destination, message.subject, message.text, message.html))
        return f"mid-{len(self.enviados)}"


def correr_modular(global_cfg, caso_cfg, ruta, plantilla, escenario, presencias, primera):
    parametros = build_parameters(
        use_case_id=caso_cfg["useCaseId"], cert_code=CERT, redirect_url=REDIRECCION,
        regeneration_without_new_data=escenario == "regenerado",
    )
    documentos = {
        (parametros.config_bucket, parametros.global_key): global_cfg,
        (parametros.config_bucket, parametros.case_key): caso_cfg,
    }
    if ruta:
        documentos[(global_cfg["buckets"]["assets"], ruta)] = plantilla
    registros = [
        NotificationRecord(
            use_case_id=i["useCaseId"], execution_id=i["executionId"], period=i["period"], status=i["status"],
            user_email=i["userEmail"], query_name=i["queryName"], file_name=i["nameFile"],
            s3_url=i["s3_url"], no_data_for_period=i["sinDatosPeriodo"],
        )
        for i in items_dynamo(caso_cfg["useCaseId"], escenario)
    ]
    notificaciones = FakeNotificationRepository(registros)
    notificador = Notificador()
    bitacora, reloj = InMemoryEventLogger(), FixedClock()
    politica = LinkPolicy(REDIRECCION, parametros.links_table, 7, 8)
    dependencias = Dependencies(
        config=FakeConfigRepository(documentos),
        notifications=notificaciones,
        notifier=notificador,
        queries=QueryService(Consultor(presencias, escenario, primera), bitacora),
        links=LinkService(FakeLinkRepository(), reloj, SequentialTokenGenerator(), bitacora, politica,
                          parametros.use_case_id, CERT),
        composite=CompositeService(FakeCompositeRepository(), reloj, bitacora),
        clock=reloj,
        logger=bitacora,
    )
    NotifyCertification(parametros, dependencias).run()
    marcados = [(len(m["records"]), m["status"], ",".join(e.message_id for e in m["sent_emails"]),
                 [e.recipient for e in m["sent_emails"]]) for m in notificaciones.marked_records]
    return notificador.enviados, marcados


def main():
    mono = cargar_monolito()
    global_cfg = leer(CFG / "previsional/config/json_global_notif_cert.json")
    total, fallas = 0, []
    for caso in CASOS:
        caso_cfg = leer(CFG / caso)
        caso_cfg.pop("compositeNotification", None)
        ruta = caso_cfg["email"].get("templatePath")
        plantilla = (TPL / ruta).read_text(encoding="utf-8") if ruta else None
        presencias = [m["query"].replace("${period}", PERIODO) for m in caso_cfg["email"].get("dataPresence", [])]
        variables = {"period": PERIODO, "env": "dev"}
        resumen = sorted(caso_cfg.get("queries", {}).get("summary", []), key=lambda q: q.get("queryName", ""))
        primera = mono.parametrizar(resumen[0]["query"], variables, unescape=True) if resumen else None
        presencias = [mono.parametrizar(m["query"], {**variables}, unescape=True) for m in caso_cfg["email"].get("dataPresence", [])]
        for escenario in ESCENARIOS:
            total += 1
            a = correr_monolito(mono, copy.deepcopy(global_cfg), copy.deepcopy(caso_cfg), plantilla, escenario, presencias, primera)
            b = correr_modular(copy.deepcopy(global_cfg), copy.deepcopy(caso_cfg), ruta, plantilla, escenario, presencias, primera)
            estado = "IGUAL" if a == b else "DISTINTO"
            con_html = sum(1 for e in a[0] if e[3])
            print(f"{estado:8} {caso:48} {escenario:18} correos={len(a[0])} con_html={con_html} plantilla={ruta}")
            if a != b:
                fallas.append((caso, escenario, a, b))
    print(f"\nCasos: {total}  distintos: {len(fallas)}")
    for caso, escenario, a, b in fallas[:2]:
        for x, y in zip(a[0], b[0]):
            for i, nombre in enumerate(["destino", "asunto", "texto", "html"]):
                if x[i] != y[i]:
                    print(f"--- {caso} {escenario} {nombre}\nMONO: {str(x[i])[:600]}\nMODU: {str(y[i])[:600]}")
        if a[1] != b[1]:
            print("marcados", a[1], b[1])


main()
