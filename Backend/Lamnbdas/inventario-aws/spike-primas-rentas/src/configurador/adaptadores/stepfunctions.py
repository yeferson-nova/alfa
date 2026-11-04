"""Adaptador de Step Functions."""
import json


class OrquestadorStep:
    def __init__(self, cliente, state_machine_arn):
        self._cliente = cliente
        self._arn = state_machine_arn

    def iniciar(self, nombre, entrada):
        respuesta = self._cliente.start_execution(
            stateMachineArn=self._arn, name=nombre, input=json.dumps(entrada)
        )
        return respuesta["executionArn"]
