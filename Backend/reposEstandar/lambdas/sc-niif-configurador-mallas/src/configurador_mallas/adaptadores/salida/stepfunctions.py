"""Adaptador de Step Functions."""
import json

from ...dominio.errores import ErrorOrquestacion
from .errores_aws import codigo_aws, protegido


class OrquestadorStepFunctions:
    def __init__(self, cliente, particion, region, cuenta):
        self._cliente = cliente
        self._base = f"arn:{particion}:states:{region}:{cuenta}:stateMachine:"

    def arn_maquina(self, maquina):
        return self._base + maquina

    @protegido("stepfunctions.iniciar")
    def iniciar(self, maquina, nombre, entrada):
        try:
            respuesta = self._cliente.start_execution(
                stateMachineArn=self.arn_maquina(maquina),
                name=nombre,
                input=json.dumps(entrada, sort_keys=True),
            )
        except Exception as exc:
            if codigo_aws(exc) == "ExecutionAlreadyExists":
                # Mismo nombre con otra entrada: un intento anterior de esta clave
                # arrancó con datos distintos. No se arranca otra.
                raise ErrorOrquestacion("Ya existe una ejecución con ese nombre y otra entrada",
                                        codigo="CFG-ORQ-001",
                                        detalle={"maquina": maquina, "nombre": nombre}) from exc
            raise
        return respuesta["executionArn"]
