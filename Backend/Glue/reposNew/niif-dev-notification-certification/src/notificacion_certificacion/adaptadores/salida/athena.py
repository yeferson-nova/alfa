import math
from typing import Any, Callable

from notificacion_certificacion.adaptadores.salida.errores_aws import traduciendo
from notificacion_certificacion.dominio.errores import ErrorConsultaFallida, ErrorPlataforma
from notificacion_certificacion.dominio.modelos import EntornoConsultas, ResultadoConsulta

ESTADO_EXITOSO = "SUCCEEDED"
ESTADOS_FALLIDOS = frozenset({"FAILED", "CANCELLED"})


class ConsultorAthena:
    def __init__(
        self,
        cliente: Any,
        esperar: Callable[[float], None],
        segundos_entre_consultas: float,
        segundos_maximos: float,
    ) -> None:
        self._cliente = cliente
        self._esperar = esperar
        self._intervalo = segundos_entre_consultas
        self._intentos = max(1, math.ceil(segundos_maximos / segundos_entre_consultas))

    def ejecutar(self, sql: str, base_datos: str, entorno: EntornoConsultas) -> ResultadoConsulta:
        identificador = self._iniciar(sql, base_datos, entorno)
        self._esperar_termino(identificador)
        return self._leer_resultado(identificador)

    def _iniciar(self, sql: str, base_datos: str, entorno: EntornoConsultas) -> str:
        with traduciendo("Athena start_query_execution"):
            respuesta = self._cliente.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Catalog": entorno.catalogo, "Database": base_datos},
                ResultConfiguration={"OutputLocation": entorno.salida},
            )
        return respuesta["QueryExecutionId"]

    def _estado(self, identificador: str) -> tuple[str, str]:
        with traduciendo("Athena get_query_execution"):
            respuesta = self._cliente.get_query_execution(QueryExecutionId=identificador)
        estado = respuesta["QueryExecution"]["Status"]
        return estado["State"], estado.get("StateChangeReason", "")

    def _esperar_termino(self, identificador: str) -> None:
        for _ in range(self._intentos):
            nombre, motivo = self._estado(identificador)
            if nombre == ESTADO_EXITOSO:
                return
            if nombre in ESTADOS_FALLIDOS:
                raise ErrorConsultaFallida("NTF-PLA-002", f"Consulta {identificador} {nombre}: {motivo}")
            self._esperar(self._intervalo)
        raise ErrorPlataforma("NTF-PLA-005", f"La consulta {identificador} excedio el tiempo maximo de espera")

    @staticmethod
    def _valores(fila: dict[str, Any]) -> tuple[str, ...]:
        return tuple(columna.get("VarCharValue", "") for columna in fila.get("Data", []))

    def _leer_resultado(self, identificador: str) -> ResultadoConsulta:
        encabezados: tuple[str, ...] | None = None
        filas: list[tuple[str, ...]] = []
        token = None
        while True:
            argumentos = {"QueryExecutionId": identificador}
            if token:
                argumentos["NextToken"] = token
            with traduciendo("Athena get_query_results"):
                respuesta = self._cliente.get_query_results(**argumentos)
            pagina = [self._valores(fila) for fila in respuesta["ResultSet"]["Rows"]]
            if encabezados is None:
                encabezados, pagina = (pagina[0] if pagina else ()), pagina[1:]
            filas.extend(pagina)
            token = respuesta.get("NextToken")
            if not token:
                return ResultadoConsulta(encabezados=encabezados, filas=tuple(filas))
