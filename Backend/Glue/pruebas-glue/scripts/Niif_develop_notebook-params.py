#%additional_python_modules openpyxl==3.0.10, pyxlsb==1.0.8

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from commons3.ExtractHelper import ExtractHelper
from pyspark.sql import functions as F

import boto3, datetime, json

s3_client = boto3.client("s3")  

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
from pyspark.sql.functions import col, concat, lit, date_format, to_date
connectname = "ALFAPRD_QA_TEST"
DB_NAME = "ALFAPRD"
DB_SCHEMA = "GENERAL"
table_name = "ANP_PERIODO_SOLICITUDES_GIRO"
partition = "PERIODO"
BUCKET_NAME = "alfa-datalake-dev-bronce"
s3_prefix = f"{DB_NAME}/{DB_SCHEMA}/{table_name}/"
s3_path = f"s3://{BUCKET_NAME}/{s3_prefix}"
connect_option = {
            "useConnectionProperties": "true",
            "dbtable": f"{DB_SCHEMA}.{table_name}",
            "connectionName": connectname,
#            "sampleQuery": "SELECT * FROM ANP_PERIODO_SOLICITUDES_GIRO WHERE PERIODO IN (202509, 202510)"
        }
input_dataframe = glueContext.create_dynamic_frame.from_options(
            connection_type = "oracle",
            connection_options = connect_option,
            transformation_ctx = "TablaExtract"
            ).toDF()                 
df_with_partition = input_dataframe.withColumn(f"{partition}_partition", date_format(to_date(concat(col(partition).cast("string"), lit("01")), 'yyyyMMdd'), 'yyyy-MM'))
df_with_partition.show(1, truncate = False)
df_with_partition.write\
    .mode("append")\
    .partitionBy(f"{partition}_partition")\
    .parquet(s3_path)
s3_path
 s3://alfa-datalake-dev-bronce/ALFAPRD/GENERAL/ANP_PERIODO_SOLICITUDES_GIRO/PERIODO_partition=2005-01/
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('datalake-dev-artefacto-concepto-nota-params')
items = [
  {
    "idArtefactoConceptoNota": "2125",
    "artefactoId": "286",
    "conceptoId": "23",
    "notaId": "85",
    "codigoArtefactoConceptoNota": "DFN_NBR_RGL_SQL",
    "fechaCreacion": "19/11/2025",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefactoConceptoNota": "2126",
    "artefactoId": "286",
    "conceptoId": "16",
    "notaId": "236",
    "codigoArtefactoConceptoNota": "DFN_QRY_ATR_DOM_EDAD",
    "fechaCreacion": "19/11/2025",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefactoConceptoNota": "2127",
    "artefactoId": "313",
    "conceptoId": "23",
    "notaId": "85",
    "codigoArtefactoConceptoNota": "DFN_NBR_RGL_SQL",
    "fechaCreacion": "19/11/2025",
    "fechaActualizacion": "",
    "eliminado": "0"
  },
  {
    "idArtefactoConceptoNota": "2128",
    "artefactoId": "313",
    "conceptoId": "16",
    "notaId": "236",
    "codigoArtefactoConceptoNota": "DFN_QRY_ATR_DOM_EDAD",
    "fechaCreacion": "19/11/2025",
    "fechaActualizacion": "",
    "eliminado": "0"
  }
]
with table.batch_writer() as batch:
    for item in items:
        batch.put_item(Item=item)
import boto3
import os
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, List, Set, Union
from collections import defaultdict
from botocore.exceptions import ClientError
logger = logging.getLogger()
logger.setLevel(logging.INFO)
DYNAMODB = boto3.resource('dynamodb')
def denormalize_and_save(artefacto_id: str):
    """
    Lee los datos normalizados para un artefacto, los denormaliza y los guarda en la tabla de cache.
    """
        
    logger.info(f"Iniciando denormalización para artefacto_id: {artefacto_id}")
    
    index_acn_table = "artefactoId-index"
    
    try:
        response = source_acn_table.query(
            IndexName=index_acn_table,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('artefactoId').eq(artefacto_id)
        )
        acn_items = response.get('Items', [])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(f"El GSI '{index_acn_table}' no fue encontrado. Por favor, créalo.")
        raise

    if not acn_items:
        logger.warning(f"No se encontraron relaciones para el artefacto_id: {artefacto_id}")
        return
        
    # 2. Recolectar todos los IDs necesarios para la lectura en lote (Batch)
    concepto_ids = list(set(item['conceptoId'] for item in acn_items if 'conceptoId' in item))
    nota_ids = list(set(item['notaId'] for item in acn_items if 'notaId' in item))

    # 3. Usar BatchGetItem para leer todos los conceptos y notas en solo dos llamadas
    try:
        batch_response = DYNAMODB.batch_get_item(
            RequestItems={
                source_concepto_table.name: {'Keys': [{'idConcepto': id} for id in concepto_ids]},
                source_nota_table.name: {'Keys': [{'idNota': id} for id in nota_ids]}
            }
        )
        conceptos_data = {item['idConcepto']: item for item in batch_response['Responses'][source_concepto_table.name]}
        notas_data = {item['idNota']: item for item in batch_response['Responses'][source_nota_table.name]}
    except ClientError as e:
        logger.error(f"Error en batch_get_item: {e}")
        return

    # 4. Obtener la información del artefacto principal
    artefacto_data = source_artefacto_table.get_item(Key={'idArtefacto': artefacto_id}).get('Item', {})

    # 5. Construir el objeto JSON denormalizado final
    parametros_list = []
    for item in acn_items:
        concepto_id = item.get('conceptoId')
        nota_id = item.get('notaId')
        artefacto_concepto_nota = item.get('idArtefactoConceptoNota')
        parametros_list.append({
            "artefacto_concepto_nota": artefacto_concepto_nota,
            "concepto": conceptos_data.get(concepto_id, {}),
            "nota": notas_data.get(nota_id, {})
        })

    final_object = {
        "caso_de_uso": artefacto_id, # La PK de la tabla de cache        
        "artefacto": artefacto_data,
        "parametros": parametros_list
    }        
    
    return final_object
# --- Referencias a las Tablas ---
source_artefacto_table = DYNAMODB.Table("datalake-dev-artefacto")
source_acn_table = DYNAMODB.Table("datalake-dev-artefacto-concepto-nota-params")
source_concepto_table = DYNAMODB.Table("datalake-dev-concepto")
source_nota_table = DYNAMODB.Table("datalake-dev-nota")
try:
    response = source_artefacto_table.scan(ProjectionExpression="idArtefacto")
    artefactos = response.get('Items', [])

    # Manejar paginación si la tabla de artefactos es grande
    while 'LastEvaluatedKey' in response:
        response = source_artefacto_table.scan(
            ProjectionExpression="idArtefacto",
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        artefactos.extend(response.get('Items', []))

except ClientError as e:
    logger.error(f"Error al escanear la tabla de artefactos: {e}")
from typing import Dict, Any, Optional, List
def _find_nota_by_id_concepto(parametros_list: List[Dict[str, Any]], concepto_id: str) -> List[str]:
    """
    Devuelve una lista con todos los 'valorNota' que coinciden con el 'idConcepto' dado.
    Si no se encuentran coincidencias, devuelve una lista vacía.
    """
    resultados = []

    for param_entry in parametros_list:
        concepto = param_entry.get('concepto', {})
        nota = param_entry.get('nota', {})

        if concepto.get('idConcepto') == concepto_id:
            valor_nota = nota.get('valorNota')
            nota_id = nota.get('idNota')
            if valor_nota and nota_id:
                resultados.append((int(nota_id), valor_nota))  # Cast para orden correcto

    # Ordenar por notaId
    resultados.sort(key=lambda x: x[0])

    # Retornar solo los valoresNota, ordenados
    return [valor for _, valor in resultados]
def get_params_drt(parametros_obj: Dict[str, Any],tipo_artefacto_id: str = "5",id_concepto_llave: str = "6", id_concepto_valor: str = "7" ) -> Optional[Dict[str, str]]:
        """
        Si el artefacto es del tipo correcto, construye un diccionario {llave: valor}
        usando los valores de nota de dos conceptos específicos.

        Por defecto, usa el valor de la nota del concepto 6 como llave y el del 7 como valor.

        Args:
            parametros_obj: El objeto denormalizado completo.
            tipo_artefacto_id: El ID del tipo de artefacto a validar.
            id_concepto_llave: El 'idConcepto' cuyo 'valorNota' será la llave del diccionario.
            id_concepto_valor: El 'idConcepto' cuyo 'valorNota' será el valor del diccionario.

        Returns:
            Un diccionario con una sola entrada {llave: valor}, o None si no se cumplen
            las condiciones.
        """
        # Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None
            
        artefacto = parametros_obj.get("artefacto", {})            
        if str(artefacto.get("tipoArtefactoId")) != str(tipo_artefacto_id):
            return None # No es el tipo de artefacto que nos interesa, salimos silenciosamente.

        # Usamos la función auxiliar para encontrar nuestras dos piezas de información
        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)
        valor = _find_nota_by_id_concepto(parametros_list, id_concepto_valor)       
        
        # Construir el resultado solo si encontramos ambas partes
        if llave and valor:
            # Usamos .lower() para que coincida exactamente con tu ejemplo "{landing: ...}"
            resultado = {llave[0].lower(): valor[0]}
            return resultado
        
        logging.warning(
            f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
            f"encontraron los conceptos para la llave (ID {id_concepto_llave}) "
            f"o el valor (ID {id_concepto_valor})."
        )
        return None
def get_params_ubi(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "6", id_concepto_llave: str = "18",id_concepto_valor: str = "7") -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})        
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                

        parametros_list = parametros_obj.get("parametros", [])          
        print(parametros_list)
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)
        valor = _find_nota_by_id_concepto(parametros_list, id_concepto_valor)     

        # 4. Construir el resultado final si encontramos todo lo necesario
        if llave and valor:
            # Filtramos la lista de valores para excluir el valor que es igual a la llave
            valores_finales = [v for v in valor if v != llave[0]]

            return {llave[0]: valores_finales}

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_arch(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "1", id_concepto_llave: str = "18",id_concepto_valor: list = ["6", "8", "19"]) -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)        
        valores = []
        for valor_id in id_concepto_valor:
            valor = _find_nota_by_id_concepto(parametros_list, valor_id)
            valores.append(valor[0])

        # 4. Construir el resultado final si encontramos todo lo necesario
        if llave and valor:
            # Filtramos la lista de valores para excluir el valor que es igual a la llave            

            return {llave[0]: {"file":valores[0] ,"ext": valores[1], "sheet": valores[2]}}

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_stru(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "2", id_concepto_llave: str = "18",id_concepto_valor: str = "10") -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.
        
        codigo = artefacto.get("codigoArtefacto")
        
        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])          
        llave = _find_nota_by_id_concepto(parametros_list, id_concepto_llave)        
        valor = _find_nota_by_id_concepto(parametros_list, id_concepto_valor)     

        # 4. Construir el resultado final si encontramos todo lo necesario
        if llave and valor:
            return {llave[0]: valor[0]}
        
        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None
def get_params_atr(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "3", id_concepto_llave: str = "18",id_concepto_valor: list = ["6","12","13","14","15","16","20","21"]) -> Optional[Dict[str, List[str]]]:
        """
        Si un artefacto es del tipo '6', extrae un caso de uso y construye una
        lista de sus carpetas asociadas (IN, OUT, ERROR, etc.).

        La lógica específica es:
        - La llave del diccionario se obtiene del 'valorNota' asociado al 'idConcepto' 18.
        - Los valores de la lista se obtienen de los 'valorNota' asociados al 'idConcepto' 7.

        Args:
            parametros_obj: El objeto denormalizado completo.

        Returns:
            Un diccionario con el formato {caso_de_uso: [lista_de_carpetas]} o None
            si no se cumplen las condiciones.
        """

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})                
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []        
        for param in parametros_list:            
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")            

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))                
        
        resultados.sort(key=lambda x: x[0])
        print(resultados)

        # Construir diccionario final
        resultado_final = {nombre.replace(" ", "_"): valor for _, nombre, valor in resultados}        
        return resultado_final
    
        if not resultado_final:
            logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                            f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
            return None
def get_params_func_arch(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8", id_concepto_llave: str = "18",id_concepto_valor: list = ["6","7","8"]) -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_ARCHIVO", "VERIFICAR_ESTRUCTURA"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])

        # Construir diccionario final
        resultado_final = {nombre.replace(" ", "_"): valor for _, nombre, valor in resultados}         
        print(resultado_final)
        
        caso_de_uso = resultado_final.get('Caso_de_uso')
        lista_atributos = resultado_final.get('Lista_Atributos')

        if lista_atributos:
            return {
                caso_de_uso: lista_atributos
            }
        else:
            ubicacion = resultado_final.get('Ubicación', '')
            nombre = resultado_final.get('Nombre', '')
            extension = resultado_final.get('Extensión', '')
            return {
                caso_de_uso: f"{ubicacion}/{nombre}{extension}"
            }         

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
def get_params_func_nul(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_NULIDAD"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                        

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])                
        
        # Separar nombres y reglas
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']  
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')


        # Emparejar regla con nombre (por posición)
        resultado_formateado = ','.join(
            f'{regla} "{nombre}"' for regla, nombre in zip(reglas, nombres)
        )
                                     

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
def get_params_func_space(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_ESPACIO"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                    

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])                
                
        
        # Separar nombres y reglas
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla'] 
        expresion = next((valor for _, tipo, valor in resultados if tipo == 'Expresion'), '') 
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        reglas_final = []
        
        for atributo in nombres: 
            
            regla_final = ' '.join([ reglas[0], atributo, reglas[1], expresion,reglas[2], atributo, reglas[3] ])            
            reglas_final.append(regla_final)
                       

        # Emparejar regla con nombre (por posición)
        print(reglas_final)
        
        resultado_formateado = ', \n'.join(reglas_final)

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
    
    
def get_params_func_type(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:
    if not isinstance(parametros_obj, dict):
        logging.error("La entrada no es un diccionario válido.")
        return None

    artefacto = parametros_obj.get("artefacto", {})
    if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any( keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_TIPO"] )):
        return None
           
    
    parametros_list = parametros_obj.get("parametros", [])
    resultados = []    

    for param in parametros_list:
        concepto = param.get("concepto", {})
        nota = param.get("nota", {})
        nombre_concepto = concepto.get("nombreConcepto")
        valor_nota = nota.get("valorNota")
        nota_id = nota.get("idNota")
        id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
        if nombre_concepto and valor_nota and nota_id:            
            resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))
    
    resultados.sort(key=lambda x: x[0])                             
        

    nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
    reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']
    tipo_dato = [valor for _, tipo, valor in resultados if tipo == 'Tipo Dato']
    restriccion = [valor for _, tipo, valor in resultados if tipo == 'Restricción']
    caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
    
    print(restriccion)
    print(nombres)
    print(reglas)

    reglas_final = []

    for idx, atributo in enumerate(nombres):        
        if idx < len(tipo_dato):
            if any('where' in r.lower() for r in reglas):
                regla_final = f"{reglas[0]} {atributo} {tipo_dato[idx]} {reglas[1]} {atributo} {reglas[2]}"                    
                reglas_final.append(str(regla_final))
            else:
                regla_final = f"{reglas[0]} {atributo} {tipo_dato[idx]}"                    
                reglas_final.append(str(regla_final))
        else:
            if any('where' in r.lower() for r in reglas):
                regla_final = f"{reglas[3]} {atributo} {restriccion[idx - len(tipo_dato)]}"                
                reglas_final.append(str(regla_final))        
            else:
                regla_final = f"{reglas[1]} {atributo} {restriccion[idx - len(tipo_dato)]}"                
                reglas_final.append(str(regla_final))

    resultado_formateado = ', \n'.join(reglas_final)

    return {caso_de_uso: resultado_formateado}
    logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
    return None    

def get_params_func_dom(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None
                
        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_DOMINIO"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                        
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []                
                

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")        
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))                
        
        resultados.sort(key=lambda x: x[0])                                        
        # Separar nombres y reglas
        
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla'] 
        dominios = [valor for _, tipo, valor in resultados if tipo == 'Dominio']         
                        
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        reglas_final = []
        
        for dominio in dominios:            
            
            regla_final = ' '.join([ reglas[0], dominio])            
            reglas_final.append(regla_final)
                                    
        # Emparejar regla con nombre (por posición)                
        resultado_formateado = ', \n'.join(reglas_final)

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
    
    
    
    
def get_params_func_sim(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None
                
        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_SIMBOLO"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                        
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []                
                

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")        
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))                
        
        resultados.sort(key=lambda x: x[0])                                        
                
        # Separar nombres y reglas
        
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre'] 
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']         
        expresiones = [valor for _, tipo, valor in resultados if tipo == 'Expresion'] 
                        
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        reglas_final = []
        
        for nombre in nombres:            
            
            regla_final = ' '.join([reglas[0], nombre, reglas[1], expresiones[0], reglas[2], nombre,reglas[3]])            
            reglas_final.append(regla_final)
                                    
        # Emparejar regla con nombre (por posición)                
        resultado_formateado = ', \n'.join(reglas_final)

        # Retornar en diccionario 
        
        return {caso_de_uso: resultado_formateado}     

        logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {id_concepto_llave}) o no había valores (ID {id_concepto_valor}).")
        return None    
    
    
    
    
def get_params_func_hist_params(parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:

        # 1. Cláusula de Guarda: Valida la entrada y el tipo de artefacto
        if not isinstance(parametros_obj, dict):
            logging.error("La entrada no es un diccionario válido.")
            return None
                
        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["HIST_PRM"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. variables para recolectar la llave y los valores                        
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []                        
                

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")        
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))                
        
        resultados.sort(key=lambda x: x[0])                                                
                
        # Separar nombres y reglas
        
        funcion = [valor for _, tipo, valor in resultados if tipo == 'Función']        
        tabla = [valor for _, tipo, valor in resultados if tipo == 'Tabla']         
        pivote = [valor for _, tipo, valor in resultados if tipo == 'Lista Atributos Pivote']                 
        periodo = [valor for _, tipo, valor in resultados if tipo == 'Lista Atributos Periodo']         
        inactivar = [valor for _, tipo, valor in resultados if tipo == 'Lista Atributos Inactivar']                                                  
                        
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        if tabla and funcion and (pivote or periodo or inactivar):                   
            return {funcion[0]: {"table_source":tabla[0] ,"pivote": pivote[0] if pivote else [], "periodo": periodo[0] if periodo else [], "inactivar": inactivar[0] if inactivar else []}}                                            
        else:
            
            logging.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {caso_de_uso}) o no había valores (ID {caso_de_uso}).")
        return None
                
       

    
    
    
    
processed_count = 0
for artefacto in artefactos:    
    artefacto_id = artefacto['idArtefacto']    
    try:
        result = None
        result = denormalize_and_save(artefacto_id)                
        if result:            
            print(f"\n== Resultado para artefacto {artefacto_id} ==")            
            #params_drt = get_params_drt(result, tipo_artefacto_id = '9')
            #params_ubi = get_params_ubi(result)
            #params_arch = get_params_arch(result)
            #params_stru = get_params_stru(result)
            #params_atr = get_params_atr(result)
            #params_func_arch = get_params_func_arch(result)
            #params_func_nul = get_params_func_nul(result)}
            #params_func_spaces = get_params_func_space(result)
            #params_func_type = get_params_func_type(result)
            #params_func_dom = get_params_func_dom(result)
            #params_func_sim = get_params_func_sim(result)
            params_func_hist_params = get_params_func_hist_params(result)
            #print(params_func_spaces)             
            #print(params_func_type)
            #print(params_func_dom)
            #print(params_func_sim)
            #print(params_func_nul)
            #print(params_func_arch)
            #print(params_atr)
            #print(params_stru)
            #print(params_arch)
            #print(params_drt)
            #print(params_ubi)
            print(params_func_hist_params)
        processed_count += 1
    except Exception as e:
        logger.error(f"Falló el procesamiento para el artefacto {artefacto_id}: {e}")
        
TABLE_NAMES = {
    'artefacto': 'datalake-dev-artefacto',
    'acn': 'datalake-dev-artefacto-concepto-nota-params',
    'concepto': 'datalake-dev-concepto',
    'nota': 'datalake-dev-nota'
}

builder = ParameterBuilder(table_names=TABLE_NAMES )

params = builder.process_and_build_all(max_workers=10)

json_params = json.loads(params)
#print(json_params)

unified_historic = builder.group_and_unify_structures(json_params, 'historic')
unified_historic
import boto3
import json
import concurrent.futures
from typing import Dict, Any, Optional, List, Set, Union
from collections import defaultdict
from botocore.exceptions import ClientError
from commons3.LogsHelper import LogsHelper
from boto3.dynamodb.conditions import Key

class ParameterBuilder():

    def __init__(self, table_names: Dict[str, str], dynamodb_client: Optional[boto3.resource] = None):

        self.dynamodb = dynamodb_client or boto3.resource('dynamodb')
        self.source_artefacto_table = self.dynamodb.Table(table_names['artefacto'])
        self.source_acn_table = self.dynamodb.Table(table_names['acn'])
        self.source_concepto_table = self.dynamodb.Table(table_names['concepto'])
        self.source_nota_table = self.dynamodb.Table(table_names['nota'])
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.logger = LogsHelper().logger_conf()
    
    def _get_all_artefact_ids(self) -> List[str]:
        """
        Scans the main artefact table to get a list of all artefact IDs to be processed.
        Handles pagination for large tables.

        Returns:
            A list of strings, where each string is an 'idArtefacto'.
        """
        try:
            self.logger.info(f"Scanning table '{self.source_artefacto_table.name}' to discover all artefacts...")
            response = self.source_artefacto_table.scan(ProjectionExpression="idArtefacto")
            artefactos = response.get('Items', [])

            # Handle pagination if the table is large
            while 'LastEvaluatedKey' in response:
                self.logger.info(f"Paginating artefact table... Found {len(artefactos)} items so far.")
                response = self.source_artefacto_table.scan(
                    ProjectionExpression="idArtefacto",
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                artefactos.extend(response.get('Items', []))
            
            self.logger.info(f"Scan complete. Found a total of {len(artefactos)} artefacts.")
            return [item['idArtefacto'] for item in artefactos]

        except ClientError as e:
            self.logger.error(f"Failed to scan artefact table: {e}")
            return []

    def _fetch_and_denormalize(self, artefacto_id: str) -> Optional[Dict[str, Any]]:
        """Fetches normalized data from multiple DynamoDB tables and assembles a single, denormalized configuration object.

        This private method orchestrates the retrieval of a complete parameter set for
        a specific `artefacto_id`. It performs the following steps:
        1.  Queries the Artefacto-Concepto-Nota (ACN) junction table using a GSI
            to find all related parameter entries.
        2.  Collects all unique 'concepto' and 'nota' IDs from the results.
        3.  Performs an efficient `batch_get_item` operation to retrieve all
            'concepto' and 'nota' items in bulk.
        4.  Fetches the main 'artefacto' item.
        5.  Combines all retrieved data into a single, denormalized dictionary
            structure, ready for caching and processing.

        Args:
            artefacto_id (str): The ID of the artefact to fetch and denormalize.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the complete
                denormalized object if successful, otherwise None.
        """
        self.logger.info(f"CACHE MISS: Leyendo y denormalizando desde DynamoDB para artefacto_id: {artefacto_id}")
        
        try:
            response = self.source_acn_table.query(
                IndexName="artefactoId-index",
                KeyConditionExpression = Key('artefactoId').eq(artefacto_id)
            )
            acn_items = response.get('Items', [])
        except ClientError as e:
            self.logger.error(f"Error de GSI al buscar artefacto_id {artefacto_id}: {e}")
            return None

        if not acn_items:
            return None
            
        concepto_ids = list(set(item.get('conceptoId') for item in acn_items if item.get('conceptoId')))
        nota_ids = list(set(item.get('notaId') for item in acn_items if item.get('notaId')))

        if not (concepto_ids and nota_ids):
             return None # No hay nada que buscar en batch

        try:
            batch_response = self.dynamodb.batch_get_item(
                RequestItems={
                    self.source_concepto_table.name: {'Keys': [{'idConcepto': id} for id in concepto_ids]},
                    self.source_nota_table.name: {'Keys': [{'idNota': id} for id in nota_ids]}
                }
            )
            conceptos_data = {item['idConcepto']: item for item in batch_response['Responses'][self.source_concepto_table.name]}
            notas_data = {item['idNota']: item for item in batch_response['Responses'][self.source_nota_table.name]}
        except ClientError as e:
            self.logger.error(f"Error en batch_get_item para artefacto_id {artefacto_id}: {e}")
            return None

        artefacto_data = self.source_artefacto_table.get_item(Key={'idArtefacto': artefacto_id}).get('Item', {})
        
        parametros_list = [
            {
                "artefacto_concepto_nota": item.get('idArtefactoConceptoNota'),
                "concepto": conceptos_data.get(item.get('conceptoId'), {}),
                "nota": notas_data.get(item.get('notaId'), {})
            }
            for item in acn_items
        ]
        
        final_object = {
            "caso_de_uso": artefacto_id,
            "artefacto": artefacto_data,
            "parametros": parametros_list
        }
        return final_object

    def _find_notes_by_concept_id(self, parametros_list: List[Dict[str, Any]], concept_id: str) -> List[str]:

        """Finds all 'valorNota' values that match a given 'idConcepto'.

        This helper method filters a list of parameter objects to find all entries
        that match a specific concept ID. It extracts the associated 'valorNota'
        for each match, sorts them based on their corresponding 'idNota', and
        returns the note values in that sorted order.

        Args:
            parametros_list (List[Dict[str, Any]]): The list of denormalized
                parameter objects to search through.
            concept_id (str): The concept ID to filter by.

        Returns:
            List[str]: A list of 'valorNota' strings, sorted by their
                corresponding 'idNota'. Returns an empty list if no matches
                are found.
        """
        # A temporary list to store found pairs of (id, value)
        results = []

        # Iterate through each parameter dictionary in the input list
        for param_entry in parametros_list:
            # Safely get the nested 'concepto' and 'nota' dictionaries
            concepto = param_entry.get('concepto', {})
            nota = param_entry.get('nota', {})

            # Check if the concept ID in the current item matches the one we're looking for
            if concepto.get('idConcepto') == concept_id:
                valor_nota = nota.get('valorNota')
                nota_id = nota.get('idNota')
                
                # Ensure both the note value and its ID exist to form a valid pair
                if valor_nota and nota_id:
                    # Cast nota_id to int for correct numerical sorting
                    results.append((int(nota_id), valor_nota))

        # Sort the results list based on the note ID (the first element of the tuple)
        results.sort(key=lambda x: x[0])

        # Extract and return only the note values (the second element) from the sorted list
        return [valor for _, valor in results]
        
    def _get_params_drt(self, parametros_obj: Dict[str, Any],  tipos_artefacto_validos: List[str] = ["5", "9"], id_concepto_llave: str = "6",  id_concepto_valor: str = "7") -> Optional[Dict[str, str]]:

        """Builds a {key: value} dictionary from the notes of two specific concepts.

        This function processes a denormalized parameter object to create a specific
        key-value pair. It first validates that the artefact is of the correct type.
        It then uses a helper function to find the note value (`valorNota`) for a
        'key' concept and a 'value' concept.

        By default, it uses the note from concept '6' as the key and the note
        from concept '7' as the value.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "5".
            id_concepto_llave (str, optional): The concept ID whose note value
                will be used as the dictionary key. Defaults to "6".
            id_concepto_valor (str, optional): The concept ID whose note value
                will be used as the dictionary value. Defaults to "7".

        Returns:
            Optional[Dict[str, str]]: A single-entry dictionary of the format
                {key: value} if all conditions are met, otherwise None.
        """
        # Guard Clause: Validate the input is a dictionary.
        if not isinstance(parametros_obj, dict):
            self.logger.error("The input is not a valid dictionary.")
            return None
            
        artefacto = parametros_obj.get("artefacto", {})
        
        # Guard Clause: Validate the artefact type.
        # Silently return None if it's not the artefact type we are interested in.
        if str(artefacto.get("tipoArtefactoId")) not in tipos_artefacto_validos:
            return None

        # Use the helper function to find our two pieces of information.
        parametros_list = parametros_obj.get("parametros", [])
        key_notes = self._find_notes_by_concept_id(parametros_list, id_concepto_llave)
        value_notes = self._find_notes_by_concept_id(parametros_list, id_concepto_valor)
        
        # Build the result only if both the key and value parts were found.
        if key_notes and value_notes:            
            # Get the first found key and value.
            key = key_notes[0]
            value = value_notes[0]            
            # Use .lower() on the key to ensure consistent formatting, e.g., "{landing: ...}"
            if str(artefacto.get("tipoArtefactoId")) == '9':
                result = {f"{key.lower()}_bdd": value}                    
            else:
                result = {key.lower(): value}
                
            return result
        
        # If either the key or value was not found, log a warning before returning None.
        self.logger.error(
            f"Artefact '{artefacto.get('nombreArtefacto')}' was valid, but required concepts "
            f"for the key (ID {id_concepto_llave}) or value (ID {id_concepto_valor}) were not found."
        )
        return None

    def _get_params_ubi(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "6",  id_concepto_llave: str = "18", id_concepto_valor: str = "7") -> Optional[Dict[str, List[str]]]:

        """Builds a dictionary mapping a use case name to a list of its folder locations.

        This function processes a denormalized parameter object to extract a use case
        name and a corresponding list of folder locations (e.g., IN, OUT, ERROR).
        It first validates that the artefact is of the correct type (default '6').
        
        The dictionary key is derived from the note of one concept (e.g., 'Caso de uso',
        default ID '18'), and the value is a list of notes from another concept
        (e.g., 'Ubicación', default ID '7').
        
        A key piece of logic is that it filters out any folder location that is
        identical to the use case name itself.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "6".
            id_concepto_llave (str, optional): The concept ID whose note value will be
                the dictionary key. Defaults to "18".
            id_concepto_valor (str, optional): The concept ID whose note values will form
                the list of dictionary values. Defaults to "7".

        Returns:
            Optional[Dict[str, List[str]]]: A dictionary mapping the use case name
                to a cleaned list of folder locations, or None if conditions aren't met.
        """
        # 1. Guard Clauses: Validate input and artefact type.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None  # Not the artefact type we are interested in, exit silently.

        # 2. Fetch the key and the list of potential values using the helper function.
        parametros_list = parametros_obj.get("parametros", [])
        key_list = self._find_notes_by_concept_id(parametros_list, id_concepto_llave)
        values_list = self._find_notes_by_concept_id(parametros_list, id_concepto_valor)

        # 3. Build the final result if all necessary parts were found.
        if key_list and values_list:
            # The key is the first (and likely only) value found for the key concept.
            key = key_list[0]
            
            # Filter the values list to exclude any value that is identical to the key itself.
            final_values = [v for v in values_list if v != key]

            return {key: final_values}

        # If either the key or values were not found, log a warning.
        self.logger.error(
            f"Artefact '{artefacto.get('nombreArtefacto')}' was valid, but required concepts "
            f"for the key (ID {id_concepto_llave}) or values (ID {id_concepto_valor}) were not found."
        )
        return None    

    def _get_params_arch(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "1",  id_concepto_llave: str = "18", id_conceptos_valores: List[str] = ["6", "8", "19"]) -> Optional[Dict[str, Dict[str, str]]]:
        """Builds a dictionary mapping a use case name to a structured dict of file properties.

        This function processes a parameter object to extract file-related properties
        for a specific use case. It validates the artefact type (default '1').
        
        The main key is derived from one concept's note (ID '18'). The value is a
        structured dictionary containing file properties ('file', 'ext', 'sheet'),
        which are derived from the notes of a list of other concept IDs
        (defaults to ['6', '8', '19']). The function assumes a fixed order for these
        value concepts.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "1".
            id_concepto_llave (str, optional): The concept ID for the main dictionary key.
                Defaults to "18".
            id_conceptos_valores (List[str], optional): A list of concept IDs whose notes will be
                fetched in order to build the structured value dictionary.
                Defaults to ["6", "8", "19"].

        Returns:
            Optional[Dict[str, Dict[str, str]]]: A dictionary mapping the use case
                to its file properties, or None if any required part is missing.
        """
        # 1. Guard Clauses: Validate input and artefact type.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None  # Not the artefact type we are interested in, exit silently.

        # 2. Fetch the main key for the dictionary.
        parametros_list = parametros_obj.get("parametros", [])
        key_list = self._find_notes_by_concept_id(parametros_list, id_concepto_llave)
        
        # 3. Sequentially fetch the values for the structured inner dictionary.
        collected_values = []
        for concept_id in id_conceptos_valores:
            # Find the note for the current concept ID
            found_value = self._find_notes_by_concept_id(parametros_list, concept_id)
            if not found_value:
                # If any component is missing, we cannot build the final object.
                self.logger.error(f"Artefact '{artefacto.get('nombreArtefacto')}' is missing required value for concept ID {concept_id}.")
                return None
            collected_values.append(found_value[0])

        # 4. Build the final result if the key and all value components were found.
        #    The original 'if llave and valor' was buggy; this is the correct check.
        if key_list and len(collected_values) == len(id_conceptos_valores):
            key = key_list[0]
            
            # Assemble the structured dictionary, assuming a fixed order.
            structured_value = {
                "file": collected_values[0],
                "ext": collected_values[1],
                "sheet": collected_values[2]
            }
            return {key: structured_value}

        if not key_list:
            self.logger.error(
                f"Artefact '{artefacto.get('nombreArtefacto')}' was valid, but the key (ID {id_concepto_llave}) "
                f"or one of the required values (IDs {id_conceptos_valores}) was not found."
            )
            return None
    
    def _get_params_stru(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "2", id_concepto_llave: str = "18", id_concepto_valor: str = "10") -> Optional[Dict[str, str]]:
        """Builds a simple key-value pair dictionary from the notes of two specific concepts.

        This function processes a parameter object to extract a simple key-value pair,
        typically representing a use case and its associated main value (e.g., a
        structure definition or a table name). It validates the artefact type
        (default '2').

        The dictionary key is derived from one concept's note (ID '18'), and the value
        is from another (ID '10').

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "2".
            id_concepto_llave (str, optional): The concept ID whose note value will be
                the dictionary key. Defaults to "18".
            id_concepto_valor (str, optional): The concept ID whose note value will be
                the dictionary value. Defaults to "10".

        Returns:
            Optional[Dict[str, str]]: A single-entry dictionary of the format
                {key: value} if successful, otherwise None.
        """
        # 1. Guard Clauses: Validate input and artefact type.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None  # Not the correct artefact type, exit silently.

        codigo = artefacto.get("codigoArtefacto")

        # 2. Fetch the key and value using the helper function.
        parametros_list = parametros_obj.get("parametros", [])
        key_list = self._find_notes_by_concept_id(parametros_list, id_concepto_llave)
        value_list = self._find_notes_by_concept_id(parametros_list, id_concepto_valor)

        # 3. Build the final result if both parts were found.
        if key_list and value_list:
            return {key_list[0]: value_list[0]}
        if not key_list: 
            self.logger.error(
                f"Artefact '{artefacto.get('nombreArtefacto')}' was valid, but required concepts "
                f"for the key (ID {id_concepto_llave}) or value (ID {id_concepto_valor}) were not found."
            )
            return None

    def _get_params_atr(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "3") -> Optional[Dict[str, str]]:
        """Extracts all parameters for a given artefact into a single dictionary.

        This function processes a parameter object for a specific artefact type
        (default '3') and aggregates all its associated parameters into a single
        key-value dictionary. The keys are the 'nombreConcepto' values (with spaces
        replaced by underscores), and the values are the corresponding 'valorNota' values.

        Note: If multiple parameters share the same 'nombreConcepto', the last one
        encountered after sorting by 'notaId' will be used.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "3".

        Returns:
            Optional[Dict[str, str]]: A dictionary containing all extracted
                key-value parameters, or None if the artefact type does not match.
        """
        # 1. Guard Clauses: Validate input and artefact type.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if str(artefacto.get("tipoArtefactoId")) != tipo_artefacto_id:
            return None  # Not the correct artefact type.

        # 2. Extract all valid parameters into a list of tuples.
        parametros_list = parametros_obj.get("parametros", [])
        
        # This list will hold tuples of (nota_id, nombre_concepto, valor_nota)
        # to be sorted and aggregated.
        all_params = []
        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})
            
            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")

            if nombre_concepto and valor_nota and nota_id:
                all_params.append((int(nota_id), nombre_concepto, valor_nota))
        
        # Sort by nota_id to ensure a consistent order.
        all_params.sort(key=lambda x: x[0])

        # 3. Build the final dictionary using a dictionary comprehension.
        #    This flattens the list into a dict. If keys are duplicated, the last
        #    one in the sorted list wins.
        final_params_dict = {
            nombre.replace(" ", "_"): valor for _, nombre, valor in all_params
        }
        
        return final_params_dict

    def _get_params_func_arch(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, Union[str, List[str]]]]:
        """Extracts verification parameters for file or structure checks.

        This function handles functional artefacts (type '8') related to file and
        structure verification. It validates that the artefact's 'codigoArtefacto'
        contains specific keywords ('VERIFICAR_ARCHIVO' or 'VERIFICAR_ESTRUCTURA').

        Based on the available parameters, it returns either a list of attributes
        or a constructed file path, keyed by the use case name.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "8".

        Returns:
            Optional[Dict[str, Union[str, List[str]]]]: A dictionary containing the
                processed parameters, or None if the artefact does not match the criteria.
        """
        # 1. Guard Clause: Validate input and perform a complex artefact check.
        if not isinstance(parametros_obj, dict):
            self.logger.error("La entrada no es un diccionario válido.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_ARCHIVO", "VERIFICAR_ESTRUCTURA"])):
            return None  # Exit if it's not the correct type OR code.

        # 2. Directly extract the required parameters instead of flattening everything.
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                        
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])

        # 3. Check for the 'Lista_Atributos' parameter to decide the return format.
        resultado_final = {nombre.replace(" ", "_"): valor for _, nombre, valor in resultados}                
        
        caso_de_uso = resultado_final.get('Caso_de_uso')
        lista_atributos = resultado_final.get('Lista_Atributos')

        if not caso_de_uso:
            self.logger.error(f"Artefacto '{artefacto.get('nombreArtefacto')}' was valid but missing required components ")
            return None

        if lista_atributos:
            return {
                caso_de_uso: lista_atributos
            }
        else:
            ubicacion = resultado_final.get('Ubicación', '')
            nombre = resultado_final.get('Nombre', '')
            extension = resultado_final.get('Extensión', '')
            return {
                caso_de_uso: f"{ubicacion}/{nombre}{extension}"
            }

    def _get_params_func_nul(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, str]]:
        """Generates a string of nullity check rules for a specific use case.

        This function handles functional artefacts (type '8') designed for nullity
        verification ('VERIFICAR_NULIDAD'). It extracts all 'Nombre' (Name) and
        'Regla' (Rule) parameters, pairs them up, and formats them into a single,
        comma-separated string, keyed by the 'Caso de uso' (Use Case) name.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "8".

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping the use case name to the
                formatted string of nullity rules, or None if conditions are not met.
        """
        # 1. Guard Clauses: Validate input and the specific artefact code.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_NULIDAD"])):
            return None  # Not the correct functional artefact, exit silently.

        # 2. Directly extract and map the required parameters.
        #    This is more robust than creating intermediate lists.
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                        

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])    

        # 3. Build the formatted string from the extracted 'Regla' and 'Nombre' params.
        #    This approach is safer than positional pairing with zip.
        #    It assumes one 'Regla' and one 'Nombre' for this specific logic.
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']  
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')

        resultado_formateado = ','.join(
            f'{regla} "{nombre}"' for regla, nombre in zip(reglas, nombres)
        )

        if not resultado_formateado:             
            self.logger.error(f"Artefact '{artefacto.get('nombreArtefacto')}' was valid but was missing 'Regla' or 'Nombre' parameters.")
            return None        

        return {caso_de_uso: resultado_formateado}     

    def _get_params_func_space(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, str]]:
        """Generates a string of space verification rules for a use case.

        This function handles functional artefacts (type '8') for space verification
        ('VERIFICAR_ESPACIO'). It extracts all 'Nombre', 'Regla', and 'Expresion'
        parameters and combines them into a formatted, multiline string, keyed by
        the 'Caso de uso' name.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "8".

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping the use case name to the
                formatted string of rules, or None if conditions are not met.
        """
        # 1. Guard Clauses: Validate input and the specific artefact code.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        codigo_artefacto = str(artefacto.get("codigoArtefacto", ""))

        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and "VERIFICAR_ESPACIO" in codigo_artefacto):
            return None

        # 2. Extract components into separate, reliable lists.
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")                    

            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(nota_id), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])                
                
        
        # Separar nombres y reglas
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla'] 
        expresion = next((valor for _, tipo, valor in resultados if tipo == 'Expresion'), '') 
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')


        # 3. Check if we have all the required parts.
        #    This logic assumes a specific structure for the 'Regla' list.
        if not (nombres and len(reglas) >= 4 and expresion and caso_de_uso):
            self.logger.error(f"Artefact '{artefacto.get('nombreArtefacto')}' was valid but missing required components (Nombres, Reglas, Expresion, or Caso de uso).")
            return None
        
        # 4. Build the final rule strings using f-strings for clarity.
        reglas_finales = []
        for atributo in nombres:
            # Construct the rule string safely without relying on fragile indices.
            # This assumes the 'reglas' list contains the rule parts in a fixed order.
            rule_string = (
                f'{reglas[0]} \"{atributo}\" {reglas[1]} {expresion}'
                f'{reglas[2]} "{atributo} {reglas[3]}"'
            )
            reglas_finales.append(rule_string)

        resultado_formateado = ', \n'.join(reglas_finales)
        return {caso_de_uso: resultado_formateado}

    def _get_params_func_type(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, str]]:

        """Generates data type verification rules for a specific use case.

        This function handles functional artefacts (type '8') for data type
        verification ('VERIFICAR_TIPO'). It reliably pairs 'Nombre' and 'Tipo Dato'
        parameters and formats them into a rule string.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "8".

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping the use case name to the
                formatted string of rules, or None if conditions are not met.
        """
        # 1. Guard Clauses: Validate input and artefact type.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any( keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["VERIFICAR_TIPO"] )):
            return None
            
        # 2. Group parameters by a common ID to create reliable pairs.
        #    This avoids the fragile positional matching problem of the original code.
        parametros_list = parametros_obj.get("parametros", [])
        resultados = []    

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})
            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            if nombre_concepto and valor_nota and nota_id:            
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))
        
        resultados.sort(key=lambda x: x[0])       
                
        # 3. Extract components and build the final rule string.
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre']
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']
        tipo_dato = [valor for _, tipo, valor in resultados if tipo == 'Tipo Dato']
        restriccion = [valor for _, tipo, valor in resultados if tipo == 'Restricción']
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')

        reglas_final = []

        if not nombres or not tipo_dato or not reglas or not caso_de_uso:
            return None
        
        # Extract the keywords for rule formatting (e.g., 'where', operators).
        for idx, atributo in enumerate(nombres):        
            if idx < len(tipo_dato):
                if any('where' in r.lower() for r in reglas):

                    regla_final = f'{reglas[0]} \"{atributo}\" = "{tipo_dato[idx]}" {reglas[1]} "{atributo} {reglas[2]}"'
                    reglas_final.append(str(regla_final))
                else:
                    regla_final = f'{reglas[0]} {atributo} = "{tipo_dato[idx]}"'                    
                    reglas_final.append(str(regla_final))
            else:
                if any('where' in r.lower() for r in reglas):
                    regla_final = f"{reglas[3]} {atributo} {restriccion[idx - len(tipo_dato)]}"                
                    reglas_final.append(str(regla_final))        
                else:
                    regla_final = f"{reglas[1]} {atributo} {restriccion[idx - len(tipo_dato)]}"                
                    reglas_final.append(str(regla_final))


        if not caso_de_uso or not reglas_final:            
            self.logger.error (f"Artefacto '{artefacto.get('nombreArtefacto')}' no es válido, pero no se ")         
            return None

        resultado_formateado = ', \n'.join(reglas_final)
                  

        return {caso_de_uso: resultado_formateado}

    def _get_params_func_dom(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, str]]:
        """Generates a string of domain verification rules for a use case.

        This function handles functional artefacts (type '8') for domain verification
        ('VERIFICAR_DOMINIO'). It extracts all 'Regla' and 'Dominio' parameters
        and formats them into a single, multiline string keyed by the 'Caso de uso' name.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "8".

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping the use case name to the
                formatted string of domain rules, or None if conditions are not met.
        """
        # 1. Guard Clauses: Validate input and the specific artefact code.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        codigo_artefacto = str(artefacto.get("codigoArtefacto", ""))
        
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and "VERIFICAR_DOMINIO" in codigo_artefacto):
            return None  # Not the correct functional artefact, exit silently.

        # 2. Directly extract the necessary components into lists.
        parametros_list = parametros_obj.get("parametros", [])
        
        # Use list comprehensions for efficient and safe extraction.
        reglas = [p.get("nota", {}).get("valorNota") for p in parametros_list if p.get("concepto", {}).get("nombreConcepto") == 'Regla' and p.get("nota", {}).get("valorNota")]
        dominios = [p.get("nota", {}).get("valorNota") for p in parametros_list if p.get("concepto", {}).get("nombreConcepto") == 'Dominio' and p.get("nota", {}).get("valorNota")]
        caso_de_uso_list = [p.get("nota", {}).get("valorNota") for p in parametros_list if p.get("concepto", {}).get("nombreConcepto") == 'Caso de uso' and p.get("nota", {}).get("valorNota")]

        # 3. Check if we have the minimum required components.
        #    This logic assumes one 'Regla' keyword and multiple 'Dominio' values.
        if not (reglas and dominios and caso_de_uso_list):
            self.logger.error(f"Artefact '{artefacto.get('nombreArtefacto')}' was valid but missing required components (Regla, Dominio, or Caso de uso).")
            return None

        caso_de_uso = caso_de_uso_list[0]
        regla_base = reglas[0]
        
        # 4. Build the final formatted string.
        #    Create a rule string for each domain found.
        reglas_finales = [f"{regla_base} {dominio}" for dominio in dominios]
        
        resultado_formateado = ', \n'.join(reglas_finales)
        
        return {caso_de_uso: resultado_formateado}

    def _get_params_func_sim(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, str]]:

        """Generates a string of symbol verification rules for a use case.

        This function handles functional artefacts (type '8') for symbol verification
        ('VERIFICAR_SIMBOLO'). It extracts 'Nombre', 'Regla', and 'Expresion'
        parameters and combines them into a formatted, multiline string.

        Args:
            parametros_obj (Dict[str, Any]): The complete denormalized parameter object.
            tipo_artefacto_id (str, optional): The artefact type ID to filter for.
                Defaults to "8".

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping the use case name to the
                formatted string of symbol rules, or None if conditions are not met.
        """
        # 1. Guard Clauses: Validate input and the specific artefact code.
        if not isinstance(parametros_obj, dict):
            self.logger.error("Input object is not a valid dictionary.")
            return None

        artefacto = parametros_obj.get("artefacto", {})
        codigo_artefacto = str(artefacto.get("codigoArtefacto", ""))

        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and "VERIFICAR_SIMBOLO" in codigo_artefacto):
            return None

        # 2. Extract components into separate, reliable lists.
        parametros_list = parametros_obj.get("parametros", [])


        resultados = []                
                

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")        
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))                
        
        resultados.sort(key=lambda x: x[0])                                        
                
        # Separar nombres y reglas
        
        nombres = [valor for _, tipo, valor in resultados if tipo == 'Nombre'] 
        reglas = [valor for _, tipo, valor in resultados if tipo == 'Regla']         
        expresiones = [valor for _, tipo, valor in resultados if tipo == 'Expresion'] 
                        
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        
        # 3. Check if we have all the required parts.
        #    This logic assumes the 'reglas' list contains parts of the rule in a fixed order.
        if not (nombres and len(reglas) >= 4 and expresiones and caso_de_uso):
            self.logger.error(f"Artefact '{artefacto.get('nombreArtefacto')}' was valid but missing required components (Nombres, Reglas, Expresion, or Caso de uso).")
            return None

        # 4. Build the final rule strings using f-strings for clarity and safety.
        reglas_finales = []
        for atributo in nombres:
            # Construct the rule string, assuming the 'reglas' list contains rule parts in a fixed order.
            rule_string = (
                f"{reglas[0]} {atributo} {reglas[1]} {expresiones[0]} "
                f'{reglas[2]} "{atributo} {reglas[3]}"'
            )
            reglas_finales.append(rule_string)

        resultado_formateado = ', \n'.join(reglas_finales)
        
        return {caso_de_uso: resultado_formateado}
    
    def _get_params_func_hist_params(self, parametros_obj: Dict[str, Any], tipo_artefacto_id: str = "8") -> Optional[Dict[str, List[str]]]:
        """
        Dynamically extracts and structures the operational parameters for the 
        Historical Premium (HIST_PRM) ETL artifact.

        The function validates the artifact type/code and processes the denormalized list 
        of parameters to reconstruct the configuration logic (source table, pivot attributes, etc.).

        Args:
            parametros_obj: Denormalized dictionary containing the complete artifact 
                            configuration (e.g., from DynamoDB).
            tipo_artefacto_id: ID of the artifact type to validate (default is "8").

        Returns:
            A structured dictionary mapping the primary function name to its 
            full configuration map, or None if the artifact is invalid or key 
            concepts (like 'Tabla' or 'Función') are missing.
        """

        # 1. Guard Clauses: Validate input and the specific artefact code.
        if not isinstance(parametros_obj, dict):
            self.logger.error("La entrada no es un diccionario válido.")
            return None
                
        artefacto = parametros_obj.get("artefacto", {})       
        if not (str(artefacto.get("tipoArtefactoId")) == tipo_artefacto_id and any(keyword in str(artefacto.get("codigoArtefacto", "")) for keyword in ["HIST_PRM"])):
            return None # No es el tipo de artefacto que nos interesa, salimos.

        # 2. Extract components into separate, reliable lists.                    
        
        parametros_list = parametros_obj.get("parametros", [])                       
        
        resultados = []                        
                

        for param in parametros_list:
            concepto = param.get("concepto", {})
            nota = param.get("nota", {})

            nombre_concepto = concepto.get("nombreConcepto")
            valor_nota = nota.get("valorNota")
            nota_id = nota.get("idNota")        
            id_artefacto_concepto_nota = param.get("artefacto_concepto_nota", {})
            
            if nombre_concepto and valor_nota and nota_id:
                resultados.append((int(id_artefacto_concepto_nota), nombre_concepto, valor_nota))                
        
        resultados.sort(key=lambda x: x[0])                                                
                
        # Split on concepts for functions
        
        funcion = [valor for _, tipo, valor in resultados if tipo == 'Función']        
        tabla = [valor for _, tipo, valor in resultados if tipo == 'Tabla']         
        pivote = [valor for _, tipo, valor in resultados if tipo == 'Lista Atributos Pivote']                 
        periodo = [valor for _, tipo, valor in resultados if tipo == 'Lista Atributos Periodo']         
        inactivar = [valor for _, tipo, valor in resultados if tipo == 'Lista Atributos Inactivar']                                                  
                        
        caso_de_uso = next((valor for _, tipo, valor in resultados if tipo == 'Caso de uso'), '')
        
        if tabla and funcion and (pivote or periodo or inactivar):                   
            return {funcion[0]: {"table_source":tabla[0] ,"pivote": pivote[0] if pivote else [], "periodo": periodo[0] if periodo else [], "inactivar": inactivar[0] if inactivar else []}}                                            
        else:
            
            self.logger.warning(f"Artefacto '{artefacto.get('nombreArtefacto')}' era válido, pero no se "
                        f"encontró el concepto para la llave (ID {caso_de_uso}) o no había valores (ID {caso_de_uso}).")
        return None
                
    def _get_config(self, artefacto_id: str) -> Dict[str, Any]:
        """
        Gets the denormalized configuration for a given artefact_id, using an
        in-memory cache to avoid redundant database calls.

        If the configuration is not in the cache, it calls the private
        _fetch_and_denormalize method to retrieve it from DynamoDB.

        Args:
            artefacto_id (str): The ID of the artefact whose configuration is needed.

        Returns:
            The complete denormalized configuration object as a dictionary.

        Raises:
            ConfigNotFoundException: If the configuration cannot be fetched.
        """
        # Check the cache first to avoid expensive I/O
        if artefacto_id not in self._cache:
            # If not in cache, call the heavy-lifter method
            denormalized_obj = self._fetch_and_denormalize(artefacto_id)
            
            if denormalized_obj is None:
                raise ValueError (f"Could not find or build config for artefact_id: {artefacto_id}")
            
            # Store the result in the cache for future use
            self._cache[artefacto_id] = denormalized_obj
            
        return self._cache[artefacto_id]   
        
    def filtrar_y_agrupar_resultados(self, json_final: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Filters a consolidated results dictionary to keep only 'SUCCESS' statuses,
        and then groups them by the primary key within the 'params' dictionary.

        Args:
            json_final (Dict[str, Dict[str, Any]]): The final JSON output from the
                parallel processing.

        Returns:
            Dict[str, List[Dict[str, Any]]]: A dictionary where keys are the parameter
                types (e.g., 'attributes', 'func_domain') and values are lists of
                the corresponding 'params' objects.
        """
        # Use defaultdict(list) to automatically create an empty list for new group keys.
        grupos = defaultdict(list)
        
        # Iterate through each artefact's result in the input dictionary.
        for artefacto_id, resultado in json_final.items():
            
            # 1. Filter: Keep only the entries with 'SUCCESS' status.
            if resultado.get('status') == 'SUCCESS':
                
                # Safely get the 'params' dictionary for this artefact.
                params = resultado.get('params', {})
                
                # Ensure the params dictionary is not empty.
                if params:
                    # 2. Group: Determine the group key (the first key inside 'params').
                    #    Example: For {'attributes': {...}}, the group key is 'attributes'.
                    group_key = list(params.keys())[0]
                    
                    # Append the entire params object to the correct group list.
                    grupos[group_key].append(params)
                    
        # Convert defaultdict back to a regular dict for the final output.
        return dict(grupos)
        
    def process_and_build_all(self, max_workers: int = 10) -> Dict[str, Any]:

        """
        Discovers all artefacts, processes them in parallel, and returns a single
        consolidated dictionary with all built parameters. This is the main
        entry point for a Glue job.
        """
        self.logger.info("Starting the full parameter building process...")
        
        # Step 1: Discover all artefacts to be processed.
        artefacto_ids = self._get_all_artefact_ids()
        if not artefacto_ids:
            self.logger.error("No artefacts found to process.")
            return {}

        # Step 2: Process the discovered artefacts in parallel.
        final_results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_artefact_id = {
                executor.submit(self.build_params_for_artefact, aid): aid
                for aid in artefacto_ids
            }
            
            for future in concurrent.futures.as_completed(future_to_artefact_id):
                artefact_id = future_to_artefact_id[future]
                try:
                    result_data = future.result()
                    if result_data:
                        final_results[artefact_id] = {"status": "SUCCESS", "params": result_data}
                    else:
                        final_results[artefact_id] = {"status": "NO_PARAMS_GENERATED"}
                except Exception as exc:
                    self.logger.error(f"Artefact {artefact_id} generated an exception: {exc}")
                    final_results[artefact_id] = {"status": "ERROR", "reason": str(exc)}
                    
        self.logger.info("Full parameter building process finished.")
        return json.dumps(self.filtrar_y_agrupar_resultados(final_results), indent=2, ensure_ascii=False) 
    
    def build_params_for_artefact(self, artefacto_id: str) -> Optional[Dict[str, Any]]:
            """
            The "worker" method. For a single artefact, it exhaustively tries to
            build every possible type of parameter set.
            """
            try:
                # The worker first gets all the data for its assigned artefact.
                config = self._get_config(artefacto_id)
            except ValueError as e:
                self.logger.error(e)
                return None
            
            # Then, the worker tries to run every builder function.
            # Each builder function is responsible for checking if it applies.
            all_params = {
                "directory": self._get_params_drt(config),
                "location": self._get_params_ubi(config),
                "file": self._get_params_arch(config),
                "structure": self._get_params_stru(config),
                "attributes": self._get_params_atr(config),
                "func_file": self._get_params_func_arch(config),
                "func_null": self._get_params_func_nul(config),
                "func_spaces": self._get_params_func_space(config),
                "func_type": self._get_params_func_type(config),
                "func_domain": self._get_params_func_dom(config),
                "func_symbol": self._get_params_func_sim(config),
                "historic": self._get_params_func_hist_params(config),
            }            
            # The worker returns a clean package of only the results that were generated.
            return {key: value for key, value in all_params.items() if value is not None}
        
    def group_and_unify_structures(self, data: List[Dict[str, Any]], artefact: str) -> Dict[str, List[str]]:

        resultado_agrupado = defaultdict(list)

        for item in data[artefact]:
            structure_dict = item.get(artefact, {})
            for clave, valor_interno in structure_dict.items():
                try:
                  if isinstance(structure_dict, dict):
                      resultado_agrupado[clave].append(valor_interno)
                  else:
                      resultado_agrupado[clave].add(valor_interno)
                except json.JSONDecodeError:
                    print(f"Advertencia: Se encontró un string de JSON mal formado para '{clave}' y fue omitido.")
                    continue

        resultado_final = {}
        for clave, valor_set in resultado_agrupado.items():
            if len(valor_set) == 1:
                # Si el set solo tiene un elemento, lo extraemos ("desempaquetamos").
                # .pop() saca y devuelve el único elemento del set.
                resultado_final[clave] = valor_set.pop()
            elif isinstance(valor_set, dict): 
                resultado_final = resultado_agrupado
            else:
                # Si hay más de un elemento, lo dejamos como una lista ordenada.
                try:
                    resultado_final[clave] = sorted(list(valor_set))
                except TypeError:
                    resultado_final[clave] = list(valor_set)
        # --- FIN DE LA MODIFICACIÓN CLAVE ---

        return resultado_final

    def group_and_transform_attributes(self,data: List[Dict[str, Any]],artefact: str) -> Dict[str, List[Dict[str, str]]]:

        # Usamos defaultdict(list) para agrupar fácilmente los atributos por caso de uso.
        resultado_agrupado = defaultdict(list)

        # 1. Iterar sobre cada diccionario en la lista de datos de entrada.
        for item in data[artefact]:
            # Obtener el diccionario de propiedades interno de forma segura.
            item_attr = item[artefact]
            # 2. Extraer los datos necesarios.
            caso_de_uso = item_attr.get('Caso_de_uso')
            nombre = item_attr.get('Nombre')
            tipo_dato = item_attr.get('Tipo_Dato')            
            # Procesar solo si tenemos la información mínima requerida.
            if caso_de_uso and nombre and tipo_dato:
                # 3. Transformar al nuevo formato con claves 'NAME' y 'TYPE'.
                nuevo_atributo = {
                    'NAME': nombre,
                    'TYPE': tipo_dato
                }
                # 4. Agregar el nuevo atributo a la lista de su caso de uso.
                resultado_agrupado[caso_de_uso].append(nuevo_atributo)

        # 5. Ordenar las listas de atributos para cada caso de uso.
        resultado_final_ordenado = {}
        for caso, lista_atributos in resultado_agrupado.items():
            # Ordenamos la lista de diccionarios usando el valor de la clave 'NAME'.
            resultado_final_ordenado[caso] = sorted(lista_atributos, key=lambda x: x.get('NAME', ''))

        return resultado_final_ordenado

    def create_rules(self, data: List[Dict[str, Any]], artefact: list) -> Dict[str, List[Dict[str, str]]]:
        """
        it finds comma-separated rule strings, splits them into individual rules, and groups them into a
        list under their corresponding use case name.

        Args:
            data (List[Dict[str, Any]]): The input data structure, expected to be a
                dictionary where keys are artefact types.
            artefact (list): A list of artefact keys (e.g., 'func_null', 'func_domain')
                to be processed.
        Returns:
            Dict[str, List[Dict[str, str]]]: A dictionary where each key is a use
                case name and the value is a consolidated list of all its
                associated rule strings.
        """
        # Group rules
        reglas_por_caso_de_uso = defaultdict(list)

        # 1. Iterate over each dictionary in the input data list.
        for item_tipo in data:
            if item_tipo in artefact:
                for item in data[item_tipo]:
                    structure_dict = item.get(item_tipo, {})
                    for caso_de_uso, reglas_string in structure_dict.items():
                        # Split the comma-separated string into individual rules
                        reglas_individuales = [
                            regla.strip() 
                            for regla in reglas_string.split(',') 
                            if regla.strip()  # Ensures empty strings are not added
                        ]

                        # 7. Consolidate: add the list of rules to the corresponding use case.
                        reglas_por_caso_de_uso[caso_de_uso].extend(reglas_individuales)

        return dict(reglas_por_caso_de_uso)
# -----------------------------
# Imports y Configuración
# -----------------------------
"""Script that validator and load bronce phase"""
import inspect, json, os, sys, tempfile, traceback, zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
import boto3
import ast
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, explode, when
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType)

from commons3.ExtractHelper import ExtractHelper
from commons3.LogsHelper import LogsHelper
#from commons3.ParameterBuilder import ParameterBuilder

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################
logs = LogsHelper()
logger = logs.logger_conf()


tables_params = {
    'artefacto': 'datalake-dev-artefacto',
    'acn': 'datalake-dev-artefacto-concepto-nota-params',
    'concepto': 'datalake-dev-concepto',
    'nota': 'datalake-dev-nota'
}

builder = ParameterBuilder(table_names=tables_params )
params = builder.process_and_build_all(max_workers=10)
json_params = json.loads(params)

#print(json_params)

# Call function to process and group data
unified_buckets = builder.group_and_unify_structures(json_params, 'directory')
unified_structures = builder.group_and_unify_structures(json_params, 'structure')
unified_files = builder.group_and_unify_structures(json_params, 'file')
unified_locations = builder.group_and_unify_structures(json_params, 'location')
unified_attributes = builder.group_and_transform_attributes(json_params, 'attributes')
unified_rules = builder.create_rules(json_params, ['func_domain','func_type','func_symbol', 'func_spaces', 'func_null'])

print(unified_rules)
print("-------")
print(unified_rules['NIIF17_ULAES_GENERALES'])
expected_columns = {
    "NIIF17_ULAES_VIDA": unified_structures['NIIF17_ULAES_VIDA'],
    "NIIF17_ULAES_GENERALES": unified_structures['NIIF17_ULAES_GENERALES'],
    "NIIF17_PREVISIONAL": unified_structures['NIIF17_PREVISIONAL']
}

files = unified_files
BUCKET_NAME = unified_buckets['bronce']
BUCKET_SOURCE = unified_buckets['landing']
BUCKET_SILVER = unified_buckets['silver']
location_logs = unified_locations['LOGS']
columns_logs = ast.literal_eval(unified_structures['LOGS'])

path_log = f"s3://{BUCKET_SILVER}/LOGS/{location_logs}/"


def build_schemas_with_order(fields_map,ordered_columns_map):
    type_mapping = {
        "VARCHAR": StringType(),
        "CHAR": StringType(),
        "NUMBER": IntegerType(),
        "NUMERIC": DoubleType(),
        "DATE": DateType()
    }

    schema_output = {}

    for table_name, fields in fields_map.items():
        # Convertir lista string a lista real
        ordered_cols = ast.literal_eval(ordered_columns_map.get(table_name, "[]"))

        # Mapear tipos por nombre
        type_dict = {f["NAME"]: type_mapping.get(f["TYPE"].upper(), StringType()) for f in fields}

        # Construir schema ordenado (solo si las columnas están en el dict)
        ordered_schema = {col: type_dict[col] for col in ordered_cols if col in type_dict}

        schema_output[table_name] = ordered_schema

    return schema_output

schema_output = build_schemas_with_order(unified_attributes, unified_structures)

#point of validation params
logger.info(f"Bucket_name_bronce: {BUCKET_NAME}")
logger.info(f"Bucket_name_landing: {BUCKET_SOURCE}")
logger.info(f"Bucket_name_silver: {BUCKET_SILVER}")
logger.info(f"Expected columns: {expected_columns}")
logger.info(f"schema columns logs: {columns_logs}")
logger.info(f"Unified locations: {unified_locations}")
logger.info(f"FILES: {files}")
logger.info(f"SCHEMA: {schema_output}")


# Initialize a global dataframe to store logs with enhanced structure
log_df = pd.DataFrame(columns=columns_logs)

# -----------------------------
# Funciones de Logging Base
# -----------------------------
def log_validation_failure(caso_de_uso, columna, indice, fallo, origen_id_lote=None, estado=0):
    """
    Enhanced logging function that automatically detects the calling function name
    and logs validation failures with detailed information.
    """
    global log_df
    
    frame = inspect.currentframe()
    try:
        caller_frame = frame.f_back
        regla_func_name = caller_frame.f_code.co_name
        
        if regla_func_name == '<lambda>':
            caller_caller_frame = caller_frame.f_back
            if caller_caller_frame:
                regla_func_name = caller_caller_frame.f_code.co_name
    except:
        regla_func_name = "unknown_function"
    finally:
        del frame
    
    #timestamp = datetime.now().isoformat()
    timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
    
    new_log = {
        "caso_de_uso": caso_de_uso,
        "columna": columna,
        "indice": indice,
        "fecha_ejecucion": timestamp,
        "origen_id_lote": origen_id_lote,
        "estado": estado,
        "fallo": fallo,
        "regla": regla_func_name
    }
    
    log_df = pd.concat([log_df, pd.DataFrame([new_log])], ignore_index=True)
    logger.info(f"LOG REGISTERED: {regla_func_name} - {columna}[{indice}]: {fallo}")

def log_row_validation_failure(df_dataquality, case_use):
    """
    Process DataFrame of DataQuality  and write errors usando log_validation_failure.
    """
    
    df_errores_pandas = df_dataquality.toPandas()
    
    for _, row in df_errores_pandas.iterrows():
        log_validation_failure(
            caso_de_uso =f"{case_use}",
            columna =row.get("rule_type", "columna_desconocida"),
            indice =int(row.get("NO_REGISTRO", -1)),
            fallo =row.get("DataQualityRulesFail", "Sin detalle de fallo"),
            estado =0
        )

def save_logs_to_s3_parquet(s3_client, s3_path):
    """
    Function to save logs to S3 in Parquet format.
    """
    global log_df
    
    if log_df.empty:
        logger.info("No logs to save.")
        return
    
    try:
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        path_without_prefix = s3_path[5:]
        bucket, key = path_without_prefix.split('/', 1)
        
        # Convert pandas DataFrame to PyArrow Table
        table = pa.Table.from_pandas(log_df)
        
        # Write to BytesIO buffer
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_content = parquet_buffer.getvalue()
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_content,
            ContentType='application/octet-stream'
        )
        
        logger.info(f"Logs successfully saved to Parquet: {s3_path}")
        parquet_buffer.close()
        
    except Exception as e:
        logger.info(f"Error saving logs to S3 Parquet: {str(e)}")
        raise

def save_consolidated_logs(s3_client, table_name=None):
    """
    Saves all accumulated logs to a single consolidated file in S3 in Parquet format only.
    """
    global log_df
    
    if log_df.empty:
        logger.info("No logs to save.")
        return
    
    try:
        timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
        table_suffix = f"_{table_name}" if table_name else ""
        
        # Save as Parquet format only
        consolidated_log_path_parquet = f"{path_log}consolidated_logs{table_suffix}_{timestamp}.parquet"
        save_logs_to_s3_parquet(s3_client, consolidated_log_path_parquet)
        logger.info(f"All logs consolidated and saved to Parquet: {consolidated_log_path_parquet}")
        
        # Clear logs after saving to prevent accumulation
        clear_logs()
        
    except Exception as e:
        logger.info(f"Error saving consolidated logs: {str(e)}")
        raise

def clear_logs():
    """
    Clears the global log DataFrame to prevent accumulation between executions.
    """
    global log_df
    log_df = pd.DataFrame(columns=log_columns)
    logger.info("Log DataFrame cleared.")

def get_log_summary():
    """
    Returns a summary of the current logs for debugging purposes.
    """
    global log_df
    if log_df.empty:
        return "No logs recorded."
    
    summary = f"""
    Total logs: {len(log_df)}
    Failed validations: {len(log_df[log_df['cumplio'] == 0])}
    Successful validations: {len(log_df[log_df['cumplio'] == 1])}
    Unique rules: {log_df['regla'].nunique()}
    Unique columns: {log_df['columna'].nunique()}
    """
    return summary
# -----------------------------
# Utilidades
# -----------------------------

def filtrar_reglas_por_tipo(rules_list, tipos_regla):
    """
    Filtra las reglas que contienen alguno de los tipos dados.
    """
    reglas_filtradas = []
    for regla in rules_list:
        regla_limpia = regla.strip().rstrip(",")
        if any(tipo in regla_limpia for tipo in tipos_regla):
            reglas_filtradas.append(regla_limpia)
    return reglas_filtradas

def save_format_file(s3_client, df_spark, path_files):
    """
    Saves the processed DataFrame as Format file in Parquet format (.parquet) in the specified S3 path.
    """
    try:
        
        timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")

        path_save_files = f"{path_files}/fecha_carga={timestamp}/"
        
        # Save the DataFrame in Parquet format to the output path
        logger.info(f'Saving the file in Parquet format to {path_save_files}')
        df_spark.write.mode("overwrite").parquet(path_save_files)
        logger.info('File successfully saved in Parquet format')
        
        logger.info(f"Parquet file successfully saved to: {path_save_files}")
            
    except Exception as e:
        logger.info(f"Error saving Format Parquet file: {str(e)}")
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="format_file", 
            indice=0, 
            fallo=f"Error al guardar archivo Format Parquet: {str(e)}",
            estado=0
        )
        raise

# -----------------------------
# Funciones de Procesamiento
# -----------------------------
def excel_s3_to_spark_df(s3_client, bucket, key_xlsx, spark, glueContext, case_use):
    """
    Convierte archivo Excel de S3 a Spark DataFrame con logging integrado.
    """
    
    try:
        excel_file_path = f"s3://{bucket}/{key_xlsx}"
        
        df = pd.read_excel(excel_file_path)
        
        df.insert(0, "NO_REGISTRO", range(2, 2 + len(df)))
        
        for col in df.columns:
            df[col] = df[col].fillna("").astype(str)
                        

        df_spark = spark.createDataFrame(df)

        return df_spark

    except FileNotFoundError:
        logger.info("File Not Found")
        raise
    
    except Exception as e:
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="proceso", 
            indice=0, 
            fallo=f"Error al procesar archivo Excel: {str(e)}",
            estado=0
        )
        raise

def glue_quality(df, rules):
    """
    Ejecuta evaluación de calidad de datos usando AWS Glue.
    """
    try:
        EvaluateDataQuality_ruleset = f"""Rules = [{rules}]"""
        
        EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
            frame=df,
            ruleset=EvaluateDataQuality_ruleset,
            publishing_options={
                "dataQualityEvaluationContext": f"DataQuality_functions",
                "enableDataQualityMetrics": True,
                "enableDataQualityResultsPublishing": False,
            },
            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
        )      
        
        ruleOutcomes = SelectFromCollection.apply(
            dfc=EvaluateDataQualityMultiframe,
            key="rowLevelOutcomes",
            transformation_ctx="rowLevelOutcomes",
        )
        
        return ruleOutcomes.toDF()
    
    except Exception as e:
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="calidad", 
            indice=0, 
            fallo=f"Error en evaluación de calidad: {str(e)}",
            estado=0
        )
        raise

# -----------------------------
# Funciones de Validación
# -----------------------------
def verificar_archivo(s3_client, bucket, key, case_use):
    """
    Verifica si un archivo existe en un bucket de S3.
    """
    try:
        logger.info(f"Checking file existence: s3://{bucket}/{key}")
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"SUCCESS: The file '{key}' exists in the bucket '{bucket}'.")
        
        return True
        
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"ERROR: The file '{key}' does not exist in the bucket '{bucket}'.")
        logger.info(f"Full path checked: s3://{bucket}/{key}")
        
        return False
        
    except Exception as e:
        logger.info(f"Unexpected error accessing the file in S3: {str(e)}")
        logger.info(f"Full path checked: s3://{bucket}/{key}")
        logger.info(f"Error type: {type(e).__name__}")
        
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="archivo_verificacion", 
            indice=0, 
            fallo=f"Error inesperado al verificar archivo: {str(e)}",
            estado=0
        )
        
        return False

def verificar_estructura_archivo(df, expected_columns,  case_use, s3_client=None):
    """
    Verifica si el DataFrame tiene las columnas esperadas en el orden correcto.
    """
    try:
        
        columnas_actuales = [col if col not in [None, ''] else 'VACIA' for col in df.columns]
        
        missing_columns = [col for col in expected_columns if col not in columnas_actuales]
        extra_columns = [col for col in columnas_actuales if col not in expected_columns]
        
        errores = []
        
        if missing_columns:
            error_msg = f"Faltan columnas requeridas: {', '.join(missing_columns)}"
            errores.append(error_msg)
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="columnas_faltantes", 
                indice=0, 
                fallo=error_msg,
                estado=0
            )
    
        if extra_columns:
            error_msg = f"Columnas adicionales no esperadas: {', '.join(extra_columns)}"
            errores.append(error_msg)
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="columnas_extra", 
                indice=0, 
                fallo=error_msg,
                estado=0
            )
     
        for idx, col in enumerate(expected_columns):
            if idx >= len(columnas_actuales) or columnas_actuales[idx] != col:
                error_msg = (
                    f"Columna esperada en posición {idx}: '{col}', "
                    f"encontrada: '{columnas_actuales[idx] if idx < len(columnas_actuales) else 'N/A'}'"
                )
                errores.append(error_msg)
                
                log_validation_failure(
                    caso_de_uso = f"{case_use}",
                    columna=f"estructura_pos_{idx}",
                    indice=0,
                    fallo=error_msg,
                    estado=0
                )
     
        if errores:
            logger.info("Invalid file structure:")
            for err in errores:
                logger.info(f"   - {err}")
            
            log_validation_failure(
                caso_de_uso = f"{case_use}",
                columna="estructura_archivo",
                indice=0,
                fallo=f"Estructura de archivo inválida. Errores encontrados: {len(errores)}",
                estado=0
            )
            
            logger.error("Invalid column structure.")
            
            return False
        else:
            
            return True
            logger.info("File structure validated successfully.")
            
    except Exception as e:
        log_validation_failure(
            caso_de_uso = f"{case_use}",
            columna="estructura_validacion",
            indice=0,
            fallo=f"Error inesperado al validar estructura: {str(e)}",
            estado=0
        )
        
        raise

def verificar_atributo(df, rules):
    df_original = df.toDF()  # Convertimos el DynamicFrame original a Spark DF
    df_original = df_original.withColumn("__row_id__", F.monotonically_increasing_id())
 
    # Ejecutar reglas de calidad
    df_with_index = DynamicFrame.fromDF(df_original, glueContext, "df_with_index")
    df_results = glue_quality(df_with_index, rules)
    
    # Filtrar solo fallos
    df_failed = df_results.filter(F.col("DataQualityEvaluationResult") == "Failed")
    
    df_failed = df_failed.withColumn("DataQualityRulesFail", explode(col("DataQualityRulesFail")))

    return df_failed

# -----------------------------
# Funciones de disponibilidad
# -----------------------------

def move_file(s3_client, bucket_bronce, bucket_landing, source_key, case_use, file, extension, status):
    
    timestamp = datetime.now(ZoneInfo("Etc/GMT+5")).strftime("%Y-%m-%d %H:%M:%S")
    
    
    destinations = {bucket_landing: f"{case_use}/{status}/{file}/fecha_carga={timestamp}/{file}{extension}"}
    
    # if status is 'OUT', make two copies
    if status.upper() == "OUT":
        destinations.update({bucket_bronce: f"FILES/{file}/fecha_carga={timestamp}/{file}{extension}"})

    for bucket, dest_key in destinations.items():
        
        logger.info(f"BUCKET {bucket}")
        logger.info(f"BUCKET {dest_key}")

        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket_landing, 'Key': source_key},
                Key=dest_key
            )
            #s3_client.delete_object(Bucket=bucket, Key=source_key)
            logger.info(f"File moved to path {status}: s3://{bucket}/{dest_key}")
            
        except Exception as e:
            
            logger.info(f"Error moving file to {status}: {e}")


def modify_df_schema_for_rules(df: DataFrame):
    """
    modify schema a dataframe allow schema 
    """
    if "MES_DE_CIERRE" in df.columns:
        df = df.withColumn("MES_DE_CIERRE", regexp_replace ("MES_DE_CIERRE", r"\.0\b", ""))
        
    if "CODRAMO" in df.columns:
        df = df.withColumn("CODRAMO", regexp_replace ("CODRAMO", r"\.0\b", ""))

    if "PERIODO" in df.columns:
        df = df.withColumn("PERIODO", regexp_replace ("PERIODO", r"\.0\b", ""))
    
    if "POLIZA" in df.columns:
        df =df.filter(lower(trim(col("POLIZA"))) != "total")
        
    df_schema_for_rules = df
    
    for name_col in df_schema_for_rules.columns:
        df_schema_for_rules = df_schema_for_rules.withColumn(
            name_col,
            when(col(name_col) == '', None).otherwise(col(name_col)).cast(StringType())
        )
        
    
    return df_schema_for_rules
    

def modify_df_schema(df: DataFrame, changes_schema: dict):
    """
    modify schema a dataframe allow schema 
    """
    df_schema = df.toDF()
    
    
    if "EMISION_P&G" in df_schema.columns:
        df_schema = df_schema.withColumnRenamed ("EMISION_P&G", "PRIMA_EMITIDA")
        
    df_schema = df_schema.withColumn("ID_LOTE_CARGUE", F.lit(None))
    
    for name_col, new_type in changes_schema.items():
        df_schema = df_schema.withColumn(name_col, col(name_col).cast(new_type))
        df_schema = df_schema.withColumn(name_col, when(col(name_col) == '', None).otherwise(col(name_col)))
    
    return df_schema
for case_use, file in files.items():        
    file_item = file['file']
    file_extension = file['ext']
    unified_locations_case = unified_locations[case_use]
    expected_columns_case = json.loads(expected_columns[case_use])
    expected_columns_case.insert(0, "NO_REGISTRO")
    schema_output_case = schema_output[f"{case_use}_BRZ"]
    
    # Check if the file exists before attempting to read it
    # File Dictionary - Using parameters from job configuration
    excel_file_path = f"{BUCKET_SOURCE}/{case_use}/{unified_locations_case[0]}/{file_item}{file_extension}"
    #rules_file_path = f"{BUCKET_NAME_ASSETS}/support_phase_files/niif/{file_item.lower()}_rules_tables.json"
    bronce_file_path = f"{BUCKET_NAME}/{case_use}/{file_item}"

    key_file = f"{case_use}/{unified_locations_case[0]}/{file_item}{file_extension}"
    key_rules = f"support_phase_files/niif/{file_item.lower()}_rules_tables.json"
    
    logger.info(f"Starting for case : {case_use}")
    logger.info(f"columns: {expected_columns_case}")
    
    logger.info(f"rule_case{unified_rules[case_use]}")    
    
type(ParameterBuilder())
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, trim, date_format, lit, max
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from datetime import datetime
import sys, traceback, boto3, json

from commons3.ExtractHelper import ExtractHelper
from commons3.LogsHelper import LogsHelper
from commons3.ParameterBuilder import ParameterBuilder

###################################################################
######## SET ARGS AND CONFIG COMMONS UTILS ########################
###################################################################

logs = LogsHelper()
logger = logs.logger_conf()



tables_params = {
    'artefacto': 'datalake-dev-artefacto',
    'acn': 'datalake-dev-artefacto-concepto-nota-params',
    'concepto': 'datalake-dev-concepto',
    'nota': 'datalake-dev-nota'
}

FUNCTION_TO_EVALUATE = ["Funcionalidad_1","Funcionalidad_4"]

#FUNCTION_TO_EVALUATE = json.loads(FUNCTION_TO_EVALUATE)
#tables_params = json.loads(tables_params)



#Read params from aux lib
builder = ParameterBuilder(table_names=tables_params )
params = builder.process_and_build_all(max_workers=10)
json_params = json.loads(params)

# Call function to process and group data
unified_directories = builder.group_and_unify_structures(json_params, 'directory')
unified_historic = builder.group_and_unify_structures(json_params, 'historic')

expected_historic = {key: unified_historic[key] for key in FUNCTION_TO_EVALUATE}

DB_CATALOG_GLUE_BRONCE = unified_directories['bronce_bdd']
DB_CATALOG_GLUE_PRM = unified_directories['prm_bdd']
BUCKET_PRM = unified_directories['prm']
DB_SCHEMA =  "ALFAPRD"
DB_NAME = "GENERAL"
PERIOD = '0'
VAL_PERIOD = '1'
activo = 1
inactivo = 0

if VAL_PERIOD == '1':
    VAL_PERIOD = True
if VAL_PERIOD == '0':
    VAL_PERIOD = False

logger.info(f"Catalog_bronce: {DB_CATALOG_GLUE_BRONCE}")
logger.info(f"Catalog_prm: {DB_CATALOG_GLUE_PRM}")
logger.info(f"Bucket_prm: {BUCKET_PRM}")
logger.info(f"Tables for func {FUNCTION_TO_EVALUATE}: tables and info : {expected_historic}")


# Use getOrCreate SparkContext on AWS Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('JOB')
s3_client = boto3.client("s3")

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

extract_helper = ExtractHelper(glueContext, spark, DB_SCHEMA, BUCKET_PRM, DB_NAME)

def obtain_last_date(fecha = None):
    """
    This Function help us to obtein the last cut date.
    """
    if fecha is None:
        fecha = datetime.now()
    if PERIOD == '0':    
        primer_dia_mes_actual = datetime(fecha.year, fecha.month, 1)
    if PERIOD != '0':
        year = int(PERIOD[0:4])
        month = int(PERIOD[4:6])
        primer_dia_mes_actual = datetime(year, month, 1)
    return primer_dia_mes_actual
    
    
def save_files(df, s3_prefix):
    """Function to use to save a DF"""
    try:
        frame_save = DynamicFrame.fromDF(df, glueContext, "frame_save_dynamic")
        s3_path = f"s3://{BUCKET_PRM}/{s3_prefix}"
        logger.info(f"Truncate & Saving data to: {s3_path}")
        extract_helper.delete_s3_folder(BUCKET_PRM, s3_prefix)
        extract_helper.save_files_s3(frame_save, s3_path, partition_columns)
        print(f"Success to save in {s3_path}")
    except Exception as e:
        print(f"Failed to save in {s3_path} because : {e}")    
import sys, traceback, boto3, json, ast
for function, tables in expected_historic.items():
    print(f"Start with logic on {function}")
    if isinstance(tables, dict):
        tables = [tables]        
    for table in tables:
        COLUMNAS_DINAMICAS = ["IPC", "TASA_TECNICA"]                
        execution_date = F.current_date()
        period_date = obtain_last_date()
        period_date = period_date.strftime('%Y-%m-%d')
        period_date_val = datetime.strptime(period_date, '%Y-%m-%d').strftime('%Y%m')        
        table_source = table['tabla_source']
        activo_inactivo = ast.literal_eval(table['activo'])
        inactivo = activo_inactivo['inactivo']
        activo = activo_inactivo['activo']
        periodo = json.loads(table['periodo']) if table['periodo'] else 'N/A'     
        pivote = json.loads(table['pivote']) if table['pivote'] else 'N/A'   
        inactivar = json.loads(table['inactivar']) if table['inactivar'] else 'N/A'           
        cols_info = json.loads(table['info']) if table['info'] else 'N/A' 
        #print(f"Start with load {table_source} from bronze")
        
for function, tables in expected_historic.items():
    print(f"Start with logic on {function}")
    if isinstance(tables, dict):
        tables = [tables]        
    for table in tables:
        COLUMNAS_DINAMICAS = ["IPC", "TASA_TECNICA"]                
        execution_date = F.current_date()
        period_date = obtain_last_date()
        period_date = period_date.strftime('%Y-%m-%d')
        period_date_val = datetime.strptime(period_date, '%Y-%m-%d').strftime('%Y%m')        
        table_source = table['tabla_source']
        periodo = json.loads(table['periodo']) if table['periodo'] else 'N/A'     
        pivote = json.loads(table['pivote']) if table['pivote'] else 'N/A'   
        inactivar = json.loads(table['inactivar']) if table['inactivar'] else 'N/A'   
        print(f"Start with load {table_source} from bronze")

        df_bronce = None
        try:
            df_bronce = glueContext.create_dynamic_frame.from_catalog(database=DB_CATALOG_GLUE_BRONCE, table_name=f"files_{table_source}").toDF()
            print(f"the table {table_source} is loaded from bronze")
        except Exception as e:
            df_bronce = None
            print(f"the table {table_source} bronze dont have data: {e}")

        print(f"Start with load {table_source} from prm")
        if df_bronce != None:            
            try:
                df_prm = glueContext.create_dynamic_frame.from_catalog(database=DB_CATALOG_GLUE_PRM, table_name=f"niif_prm_{table_source}").toDF()
                if df_prm.count() < 1:
                    df_prm = None
                print(f"the table {table_source} is loaded from prm")
            except Exception as e:
                df_prm = None
                print(f"the table {table_source} prm dont have data, dont loaded: {e}")
                                
            # The correct logic assumes you compare distinct variables:
            
            if function == 'Funcionalidad_1':
                # Logic for Functionality 1
                print(f"Executing logic for {function}") 
                
                max_period_new = df_bronce.agg(F.max(periodo[0])).collect()[0][0]
                df_bronce_filter = df_bronce.filter(F.col(periodo[0]) == max_period_new)
                
                if df_prm != None:
                    window_spec = Window.partitionBy(pivote[0])
                    df_prm_max_per = df_prm.withColumn(f"{periodo[0]}_MAX", F.max(periodo[0]).over(window_spec))
                    df_prm_max_per_filter = df_prm_max_per.filter(F.col(periodo[0]) == F.col(f"{periodo[0]}_MAX")).drop(f"{periodo[0]}_MAX")
                    df_prm_max_per_filter_diff = df_prm_max_per.filter(F.col(periodo[0]) != F.col(f"{periodo[0]}_MAX")).drop(f"{periodo[0]}_MAX")
                if df_prm != None:
                    df_prm.cache()
                    
                df_bronce = df_bronce.withColumn(
                    f"{periodo[0]}_DATE",
                    F.to_date(F.concat_ws("-", F.col(periodo[0]).substr(1,4), F.col(periodo[0]).substr(5,2), F.lit("01")), "yyyy-MM-dd")
                )
                                
                if df_prm == None:                    
                    print(f"This is the first execution of {function}")
                    df_bronce = df_bronce.withColumn(
                            "PER_FEC_INI_VIG" , F.trunc(F.col(f"{periodo[0]}_DATE"), "month")
                        ).withColumn(
                            "PER_FEC_FIN_VIG", F.last_day(F.col(f"{periodo[0]}_DATE"))    
                        ).withColumn(
                            "FEC_INI_VIG_REG", F.current_date()
                        )

                    w = Window.partitionBy(pivote[0]).orderBy(F.desc(periodo[0]))
                    
                    df_result = df_bronce.withColumn(
                        f"max_{periodo[0]}", F.max(periodo[0]).over(Window.partitionBy(pivote[0]))
                        ).withColumn(
                            "FEC_FIN_VIG_REG",
                            F.when(F.col(periodo[0]) == F.col(f"max_{periodo[0]}"), F.lit(None).cast("date")).otherwise(execution_date)
                        ).withColumn(
                            "ACTIVO", 
                            F.when(F.col(periodo[0]) == F.col(f"max_{periodo[0]}"), F.lit(activo)).otherwise(F.lit(inactivo))
                        ).withColumn(
                            "FEC_INI_VIG_REG",
                            F.when(F.col(periodo[0]) == F.col(f"max_{periodo[0]}"), F.date_add(F.col("FEC_INI_VIG_REG"), 1)).otherwise(F.col("FEC_INI_VIG_REG"))
                        ).drop(f"max_{periodo[0]}","parition_0","parition_1","parition_2")

                    df_result = df_result.select(periodo[0], "PER_FEC_INI_VIG", "PER_FEC_FIN_VIG", pivote[0], "IPC", "TASA_TECNICA", "FEC_INI_VIG_REG", "FEC_FIN_VIG_REG", "ACTIVO")   
                    df_result = df_result.distinct()
                    
                    s3_prefix = f"TEST/NIIF/PRM_{table_source.upper()}/"
                    partition_columns = [periodo[0]]
                    try:
                        save_files(df_result, s3_prefix)
                    except Exception as e:                        
                        logger.info(f"Failed to save table to {s3_prefix}. Error: {e}")
                        
                    logger.info(f"End First Execution Functionality {function} table {table_source}")
                
                if df_prm != None:   
                    print(f"This is the incremental execution of {function}")
                    period_val = df_prm_max_per_filter.select(periodo[0]).distinct().collect()[0][0]
                    if (period_date_val == period_val) or VAL_PERIOD == True:
                        
                        df_join = df_prm_max_per_filter.alias("ant").join(
                            df_bronce_filter.alias("new"),
                            on = pivote[0],
                            how = "outer"
                        )
                        
                        cond_new = F.col(f"ant.{pivote[0]}").isNull()
                        cond_change = F.col(f"ant.{pivote[0]}").isNotNull() & F.col(f"new.{pivote[0]}").isNotNull()
                        cond_with_out_changes = F.col(f"new.{pivote[0]}").isNull()  
                        
                        expresiones_fijas_news = [                            
                            F.coalesce(F.col(f"new.{periodo[0]}"), F.lit(None)).alias(periodo[0]),
                            F.col(f"new.{pivote[0]}").alias(pivote[0]),
                            F.lit(activo).alias("ACTIVO"),
                            (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"),
                            F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),
                            F.trunc(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                            F.last_day(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG")                        
                        ]
                        expresiones_dinamicas_news = [F.col(f"new.{col_name}") for col_name in COLUMNAS_DINAMICAS]
                        select_df_news = expresiones_fijas_news + expresiones_dinamicas_news
                        df_news = df_join.filter(cond_new & (F.col(f"new.{inactivar[0]}") == inactivo)).select(*select_df_news)

                        
                        expresiones_fijas_ant_update = [
                            F.col(f"ant.{periodo[0]}").alias(periodo[0]),
                            F.col(f"ant.{pivote[0]}").alias(pivote[0]),
                            F.lit(inactivo).alias("ACTIVO"),
                            F.col("ant.FEC_INI_VIG_REG"),
                            F.lit(execution_date).alias("FEC_FIN_VIG_REG"),
                            F.col("ant.PER_FEC_INI_VIG"),
                            F.col("ant.PER_FEC_FIN_VIG")
                        ]
                        expresiones_dinamicas_ant_update = [F.col(f"ant.{col_name}") for col_name in COLUMNAS_DINAMICAS]
                        select_df_ant_update = expresiones_fijas_ant_update + expresiones_dinamicas_ant_update                        
                        df_ant_update = df_join.filter(cond_change & (F.col(f"new.{inactivar[0]}") == activo)).select(*select_df_ant_update)                        
                        
                        
                        expresiones_fijas_change_new = [                            
                            F.col(f"new.{periodo[0]}").alias(periodo[0]),
                            F.col(pivote[0]).alias(pivote[0]),                                                        
                            F.lit(activo).alias("ACTIVO"),
                            (execution_date + F.expr("INTERVAL 1 DAY")).alias("FEC_INI_VIG_REG"), 
                            F.lit(None).cast("date").alias("FEC_FIN_VIG_REG"),     
                            F.trunc(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd"), "month").alias("PER_FEC_INI_VIG"),
                            F.last_day(F.to_date(F.concat_ws("-", F.col(f"new.{periodo[0]}").substr(1,4), F.col(f"new.{periodo[0]}").substr(5,2), F.lit("01")), "yyyy-MM-dd")).alias("PER_FEC_FIN_VIG")
                        ]

                        select_df_change_new = expresiones_fijas_change_new + expresiones_dinamicas_news
                        
                        df_change_new = df_join.filter(cond_change & (F.col(f"new.{inactivar[0]}") == inactivo)).select(*select_df_change_new)

                                                
                        expresiones_fijas_with_out_change = [                
                            F.col(f"ant.{periodo[0]}").alias(periodo[0]),
                            F.col(f"ant.{pivote[0]}").alias(pivote[0]),
                            F.col("ant.ACTIVO").alias("ACTIVO"),
                            F.col("ant.FEC_INI_VIG_REG").alias("FEC_INI_VIG_REG"), 
                            F.col("ant.FEC_FIN_VIG_REG").alias("FEC_FIN_VIG_REG"),                             
                            F.col("ant.PER_FEC_INI_VIG").alias("PER_FEC_INI_VIG"),
                            F.col("ant.PER_FEC_FIN_VIG").alias("PER_FEC_FIN_VIG"),
                        ]

                        select_df_with_out_change = expresiones_fijas_with_out_change + expresiones_dinamicas_ant_update

                        # Aplicación
                        df_with_out_change = df_join.filter(cond_with_out_changes).select(*select_df_with_out_change)      
                        
                        df_result = df_news.unionByName(df_ant_update).unionByName(df_change_new).unionByName(df_with_out_change).unionByName(df_prm_max_per_filter_diff)
                        df_result = df_result.distinct()
                        
                        s3_prefix = f"TEST/NIIF/PRM_{table_source.upper()}/"
                        partition_columns = [periodo[0]]                        
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            print(f"Failed to save table to {s3_prefix}. Error: {e}")

                        print(f"End Incremental Execution Functionality {function} table {table_source}")                        
                        
                    
            elif function == 'Funcionalidad_2':
                # Logic for Functionality 2
                print(f"Executing logic {function}") 
                
                if df_bronce != None:    
                    max_df_bronce_new = df_bronce.agg(F.max(periodo[0])).collect()[0][0]
                    df_bronce = df_bronce.filter(F.col(periodo[0]) == max_df_bronce_new)
                
                if df_prm == None:
                    df_bronce = df_bronce.withColumnRenamed(f"{periodo[0]}#0",periodo[0])
                    df_result = df_bronce.withColumn(
                        "FECHA_FIN",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "ACTIVO",
                        F.lit(activo)
                    ).withColumn(
                        "FEC_CARGUE",
                        F.lit(execution_date)
                    )          
                    df_result = df_result.distinct()                                        
                    
                    s3_prefix = f"TEST/NIIF/PRM_{table_source.upper()}/"
                    partition_columns = [periodo[0]]                    
                    try:
                        save_files(df_result, s3_prefix)
                    except Exception as e:                        
                        print(f"Failed to save table to {s3_prefix}. Error: {e}")

                    print(f"End first Execution Functionality {function} table {table_source}")     
                    
                if df_prm !=None:
                    
                    period_val = df_prm.select(periodo[0]).distinct().collect()[0][0]
                    period_val = period_val.strftime('%Y%m')
                    if (period_date_val == period_val) or VAL_PERIOD == True:
                        df_prm = df_prm.withColumn(
                                "ACTIVO",
                                F.lit(inactivo)
                            ).withColumn(
                                "FECHA_FIN",
                                F.date_sub(F.current_date(), 1)
                            )
                        
                        df_result = df_result.withColumn(
                                "FECHA_FIN",
                                F.lit(None)
                            ).withColumn(
                                "ACTIVO",
                                F.lit(activo)
                            ).withColumn(
                                "FEC_CARGUE",
                                F.lit(execution_date)
                            )

                        df_result = df_result.unionByName(df_prm)
                        df_result = df_result.distinct()

                        s3_prefix = f"TEST/NIIF/PRM_{table_source.upper()}/"
                        partition_columns = [periodo[0]]                    
                        try:
                            save_files(df_result, s3_prefix)
                        except Exception as e:                        
                            print(f"Failed to save table to {s3_prefix}. Error: {e}")

                        print(f"End Incremental Execution Functionality {function} table {table_source}")                                                                                                           

            
            elif function == 'Funcionalidad_3':
                # Logic for Functionality 3
                print(f"Executing logic {function}")
                
                if df_bronce != None and df_bronce.count() > 1:
                    max_df_bronce_fec_ini = df_bronce.agg(F.max(periodo[0])).collect()[0][0]
                    df_bronce = df_bronce.filter(F.col(periodo[0]) == max_df_bronce_fec_ini)
                
                if df_prm != None:
                    max_df_prm_fec_ini = df_prm.agg(F.max("FEC_INI_VIG")).collect()[0][0]
                    if df_prm.select("FEC_INI_VIG").distinct().count() > 1:
                        df_prm_anteriores = df_prm.filter(~(F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                        df_prm = df_prm.filter(F.col("FEC_INI_VIG") == max_df_prm_fec_ini)
                    else:
                        df_prm = df_prm.filter((F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                        df_prm_anteriores = df_prm.filter((F.col("ACTIVO") == "3"))
                        
                if df_prm == None:
                    df_result = df_bronce.withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_CARGUE",
                            F.lit(F.current_date())
                        ).withColumn(
                            "FEC_FIN_VIG",
                            F.lit(None).cast(StringType())
                        ).withColumn(
                            "FEC_INI_VIG",
                            F.col(periodo[0])
                        )

                    df_result = df_result.drop(inactivar[0],periodo[0])  
                    df_result = df_result.distinct()                    
                    
                    s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                    partition_columns = ["FEC_CARGUE"]                    
                    try:
                        save_files(df_result, s3_prefix)
                    except Exception as e:                        
                        print(f"Failed to save table to {s3_prefix}. Error: {e}")

                    print(f"End first Execution Functionality {function} table {table_source}")                       
                    
                if df_prm != None:  
                    
                    df_bronce_inactivar = df_bronce.filter(F.col(inactivar[0]) == '1')
                    df_bronce = df_bronce.filter(F.col(inactivar[0]) == '0')    
                    
                    df_result_new = df_bronce.withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_CARGUE",
                            F.lit(F.current_date())
                        ).withColumn(
                            "FEC_FIN_VIG",
                            F.lit(None).cast(StringType())
                        ).withColumn(
                            "FEC_INI_VIG",
                            F.col(periodo[0])
                        )                    
                    
                    df_ant_des = df_result_new.alias("new").join(df_prm.alias("ant"),
                        F.col(f"ant.{pivote[0]}") == F.col(f"new.{pivote[0]}"), "inner"
                    )
                        
                    df_ant_des = df_ant_des.withColumn(
                        "ACTIVO_new",
                            F.lit(str(inactivo))
                        ).withColumn(
                        "FEC_FIN_VIG_new",
                            F.date_trunc("month", F.add_months(F.current_date(), 1))
                        )
                    df_ant_des = df_ant_des.select("ant.*","ACTIVO_new","FEC_FIN_VIG_new")
                    df_ant_des = df_ant_des.drop(F.col("ant.ACTIVO"),F.col("ant.FEC_FIN_VIG"))
                    df_ant_des = df_ant_des.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")              
                    
                    df_result_new = df_result_new.filter(F.col(inactivar[0]) == '0').drop(inactivar[0],periodo[0])
                    df_ant_sin_cambios = df_prm.join(df_result_new, on=pivote[0], how="left_anti")                    
                
                    df_result_new = df_result_new.unionByName(df_ant_des).unionByName(df_prm_anteriores).unionByName(df_ant_sin_cambios)
                    df_result_new_inct = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "inner").select("New.*")
                    df_result_new = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "left_anti").select("New.*")       
                    
                    
                    df_result_new_inct = df_result_new_inct.withColumn(
                        "ACTIVO_new",
                            F.lit(str(inactivo))
                        ).withColumn(
                        "FEC_FIN_VIG_new",
                            F.date_trunc("month", F.add_months(F.current_date(), 1))
                        )
                    
                    df_result_new_inct = df_result_new_inct.select("New.*","ACTIVO_new","FEC_FIN_VIG_new")
                    df_result_new_inct = df_result_new_inct.drop(F.col("New.ACTIVO"),F.col("New.FEC_FIN_VIG"))
                    df_result_new_inct = df_result_new_inct.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")

                    df_result_new = df_result_new.unionByName(df_result_new_inct)
                    df_result.drop(F.col("NO_REGISTRO"))
                    df_result = df_result_new.distinct()
                    
                    
                    s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                    partition_columns = ["FEC_CARGUE"]                    
                    try:
                        save_files(df_result, s3_prefix)
                    except Exception as e:                        
                        print(f"Failed to save table to {s3_prefix}. Error: {e}")

                    print(f"End Incremental Execution Functionality {function} table {table_source}") 
                                                    
            elif function == 'Funcionalidad_4':
                # Logic for Functionality 3
                logger.info(f"Executing logic {function}")                
                
                if df_bronce != None and df_bronce.count() > 1:
                    max_df_bronce_fec_ini = df_bronce.agg(F.max("FECHA_CARGA")).collect()[0][0]
                    df_bronce = df_bronce.filter(F.col("FECHA_CARGA") == max_df_bronce_fec_ini)
                    
                df_prm_anteriores = None                
                if df_prm != None:
                    print(periodo)
                    print(df_prm)
                    max_df_prm_fec_ini = df_prm.agg(F.max("FEC_INI_VIG")).collect()[0][0]
                    if df_prm.select("FEC_INI_VIG").distinct().count() > 1:
                        df_prm_anteriores = df_prm.filter(~(F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                        df_prm = df_prm.filter(F.col("FEC_INI_VIG") == max_df_prm_fec_ini)
                    else:
                        df_prm = df_prm.filter((F.col("FEC_INI_VIG") == max_df_prm_fec_ini))
                        df_prm_anteriores = df_prm.filter((F.col("ACTIVO") == "3"))                                                                                        
                        
                
                if df_prm == None:
                    
                    df_result = df_bronce.withColumn(
                        "ACTIVO",
                        F.lit(activo)
                    ).withColumn(
                        "FEC_FIN_VIG",
                        F.lit(None).cast(StringType())
                    ).withColumn(
                        "FEC_INI_VIG",
                        F.current_date()
                    )

                    df_result = df_result.drop(inactivar[0])

                    df_result = df_result.distinct()
                    
                    s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                    partition_columns = ["FEC_INI_VIG"]               
                    
                    try:
                        save_files(df_result, s3_prefix)
                    except Exception as e:                        
                        print(f"Failed to save table to {s3_prefix}. Error: {e}")

                    print(f"End first Execution Functionality {function} table {table_source}")     
                                    
                if df_prm != None and df_bronce != None:
                    
                    df_bronce_inactivar = df_bronce.filter(F.col(inactivar[0]) == '1')
                    df_bronce = df_bronce.filter(F.col(inactivar[0]) == '0')

                    df_result_new = df_bronce.withColumn(
                            "ACTIVO",
                            F.lit(activo)
                        ).withColumn(
                            "FEC_FIN_VIG",
                            F.lit(None).cast(StringType())
                        ).withColumn(
                            "FEC_INI_VIG",
                            F.current_date()
                        )
                    
                    df_ant_des = df_result_new.alias("new").join(df_prm.alias("ant"),
                                F.col(f"ant.{pivote[0]}") == F.col(f"new.{pivote[0]}"), "inner"
                            )#Inactivar los que tengan datos
                    df_ant_des = df_ant_des.withColumn(
                        "ACTIVO_new",
                            F.lit(str(inactivo))
                        ).withColumn(
                        "FEC_FIN_VIG_new",
                            F.date_trunc("month", F.add_months(F.current_date(), 1))
                        )
                    df_ant_des = df_ant_des.select("ant.*","ACTIVO_new","FEC_FIN_VIG_new")
                    df_ant_des = df_ant_des.drop(F.col("ant.ACTIVO"),F.col("ant.FEC_FIN_VIG"))
                    df_ant_des = df_ant_des.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")

                    df_result_new = df_result_new.filter(F.col(inactivar[0]) == '0').drop(inactivar[0])
                    df_ant_sin_cambios = df_prm.join(df_result_new, on=pivote[0], how="left_anti")
                    df_result_new = df_result_new.unionByName(df_ant_des).unionByName(df_prm_anteriores).unionByName(df_ant_sin_cambios)
                    df_result_new_inct = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "inner").select("New.*")
                    df_result_new = df_result_new.alias("New").join(df_bronce_inactivar.alias("Inc"), on=pivote[0], how = "left_anti").select("New.*")   
                    
                    df_result_new_inct = df_result_new_inct.withColumn(
                        "ACTIVO_new",
                            F.lit(str(inactivo))
                        ).withColumn(
                        "FEC_FIN_VIG_new",
                            F.date_trunc("month", F.add_months(F.current_date(), 1))
                        )
                    df_result_new_inct = df_result_new_inct.select("New.*","ACTIVO_new","FEC_FIN_VIG_new")
                    df_result_new_inct = df_result_new_inct.drop(F.col("New.ACTIVO"),F.col("New.FEC_FIN_VIG"))
                    df_result_new_inct = df_result_new_inct.withColumnRenamed("ACTIVO_new","ACTIVO").withColumnRenamed("FEC_FIN_VIG_new","FEC_FIN_VIG")                                        

                    df_result_new = df_result_new.unionByName(df_result_new_inct)
                    df_result_new.drop(F.col("NO_REGISTRO"))
                    df_result = df_result_new.distinct()                                        
               
                    s3_prefix = f"NIIF/PRM_{table_source.upper()}/"
                    partition_columns = ["FEC_INI_VIG"]              
                    try:
                        save_files(df_result, s3_prefix)
                    except Exception as e:                        
                        print(f"Failed to save table to {s3_prefix}. Error: {e}")

                    print(f"End Incremental Execution Functionality {function} table {table_source}") 

            else:
                # Default logic (if none match)
                print(f"Function '{function}' is not configured")

job.commit()