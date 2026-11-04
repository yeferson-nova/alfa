"""Raíz de composición: arma el servicio con los adaptadores reales de AWS."""
from .config import Ajustes
from .servicio import Configurador


def crear_configurador(ajustes=None):
    import boto3  # importado aquí para que las pruebas unitarias no lo requieran

    from .adaptadores.dynamo import DynamoCnf, DynamoEjecuciones, DynamoPrs
    from .adaptadores.s3 import AlmacenS3
    from .adaptadores.stepfunctions import OrquestadorStep

    ajustes = ajustes or Ajustes.desde_entorno()
    recurso = boto3.resource("dynamodb", region_name=ajustes.region)
    dynamo = boto3.client("dynamodb", region_name=ajustes.region)
    return Configurador(
        ajustes=ajustes,
        cnf=DynamoCnf(recurso, dynamo, ajustes.cnf_table),
        prs=DynamoPrs(recurso, ajustes.prs_table),
        almacen=AlmacenS3(boto3.client("s3", region_name=ajustes.region)),
        orquestador=OrquestadorStep(
            boto3.client("stepfunctions", region_name=ajustes.region),
            ajustes.state_machine_arn,
        ),
        ejecuciones=DynamoEjecuciones(dynamo, ajustes.ejc_table),
    )
