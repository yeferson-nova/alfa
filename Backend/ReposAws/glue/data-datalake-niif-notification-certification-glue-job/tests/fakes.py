from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from notificacion_certificacion.dominio.modelos import RegistroCompuesto, ResultadoConsulta


class RelojFijo:
    def __init__(self) -> None:
        self.local = datetime(2026, 11, 4, 10, 0, 0, tzinfo=ZoneInfo("America/Bogota"))
        self.utc = datetime(2026, 11, 4, 15, 0, 0, tzinfo=timezone.utc)

    def ahora_local(self) -> datetime:
        return self.local

    def ahora_utc(self) -> datetime:
        return self.utc


class GeneradorSecuencial:
    def __init__(self) -> None:
        self.contador = 0

    def generar(self, longitud: int) -> str:
        self.contador += 1
        return str(self.contador).zfill(longitud)


class BitacoraEnMemoria:
    def __init__(self) -> None:
        self.eventos: list[tuple[object, str, dict]] = []

    def registrar(self, evento, nivel="INFO", **contexto) -> None:
        self.eventos.append((evento, nivel, contexto))

    def nombres(self) -> list:
        return [evento for evento, _, _ in self.eventos]


class RepositorioConfiguracionFalso:
    def __init__(self, documentos: dict) -> None:
        self.documentos = documentos
        self.lecturas: list[tuple[str, str]] = []

    def cargar_json(self, bucket: str, clave: str) -> dict:
        self.lecturas.append((bucket, clave))
        return self.documentos[(bucket, clave)]

    def cargar_texto(self, bucket: str, clave: str) -> str:
        self.lecturas.append((bucket, clave))
        contenido = self.documentos[(bucket, clave)]
        if isinstance(contenido, Exception):
            raise contenido
        return contenido


class RepositorioNotificacionesFalso:
    def __init__(self, registros) -> None:
        self.registros = tuple(registros)
        self.busquedas: list[tuple] = []
        self.marcados: list[dict] = []

    def buscar_por_cert_code(self, tabla, indice, cert_code, caso_uso):
        self.busquedas.append((tabla, indice, cert_code, caso_uso))
        return self.registros

    def marcar_enviados(self, tabla, registros, estado, canal, envios, fecha_envio) -> None:
        self.marcados.append(
            {
                "tabla": tabla,
                "registros": tuple(registros),
                "estado": estado,
                "canal": canal,
                "envios": tuple(envios),
                "fecha_envio": fecha_envio,
            }
        )


class RepositorioEnlacesFalso:
    def __init__(self) -> None:
        self.guardados: list[tuple[str, object]] = []

    def guardar(self, tabla, enlace) -> None:
        self.guardados.append((tabla, enlace))


class ConsultorFalso:
    def __init__(self, respuestas: dict[str, ResultadoConsulta | Exception]) -> None:
        self.respuestas = respuestas
        self.llamadas: list[tuple] = []

    def ejecutar(self, sql, base_datos, entorno) -> ResultadoConsulta:
        self.llamadas.append((sql, base_datos, entorno))
        respuesta = self.respuestas[sql]
        if isinstance(respuesta, Exception):
            raise respuesta
        return respuesta


class NotificadorFalso:
    def __init__(self, ids: list[str] | None = None) -> None:
        self.ids = ids
        self.enviados: list[tuple] = []

    def enviar(self, mensaje, destinatario) -> str:
        self.enviados.append((mensaje, destinatario))
        if self.ids is not None:
            return self.ids[len(self.enviados) - 1]
        return f"id-{len(self.enviados)}"


class RepositorioCompuestoFalso:
    def __init__(self, registros: dict[tuple[str, str, str], RegistroCompuesto] | None = None) -> None:
        self.registros = registros or {}
        self.consumos: list[tuple] = []

    def obtener(self, tabla, id_compuesto, id_miembro):
        return self.registros.get((tabla, id_compuesto, id_miembro))

    def consumir(self, reserva, id_mensaje, fecha) -> None:
        self.consumos.append((reserva, id_mensaje, fecha))
