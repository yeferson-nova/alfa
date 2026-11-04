import json
import os
from datetime import datetime 
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    
    FOLDERS = os.environ['FOLDERS'].split(',')#.get('FOLDERS')
    bucket = os.environ['BUCKET']#.get('BUCKET')

    fecha_actual = datetime.utcnow()#.strftime("%Y-%m-%d")

    for folder in FOLDERS:
        try:
            carpetas = s3.list_objects_v2(Bucket=bucket, Prefix=folder+'/', Delimiter='/')
            for carpeta in carpetas.get('CommonPrefixes'):
                if carpeta.get('Prefix') != folder:
                    prefix = f"{carpeta.get('Prefix')}{fecha_actual.year}/{fecha_actual.month}/{fecha_actual.day}"
                    respuesta = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                    if 'Contents' in respuesta:
                        obj_borrar = [{"Key": obj["Key"]} for obj in respuesta["Contents"]]
                        print(f'Se ba ha borrar un objeto en {prefix}')
                        try:
                            s3.delete_objects(Bucket=bucket, Delete={"Objects": obj_borrar})
                        except Exception as e:
                            print(e)
            #print(carpetas)
        except Exception as e:
            print(e)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
