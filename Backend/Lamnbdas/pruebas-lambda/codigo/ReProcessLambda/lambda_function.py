import json
import os

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    # TODO implement
    bucket_name = os.environ['BUCKET_NAME']
    
    TABLE = os.environ['TABLE']
    year = os.environ['YEAR']
    month = os.environ['MONTH']
    day = os.environ['DAY']
    environment = os.environ['ENVIRON']
    
    if environment == 'bronce':
        folder_to_delete = f"s3://{bucket_name}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-{TABLE}/{year}/{month}/{day}/"    
    if environment == 'silver':
        folder_to_delete = f"s3://{bucket_name}/ACSELE/TRANSFORMATION/ACSELE_{TABLE}/{year}/{month}/{day}/"
    if environment == 'gold':
        folder_to_delete = f"s3://{bucket_name}/ACSELE/ACSELE_ALFA/ACSELE-ACSELE_ALFA-{TABLE}/{year}/{month}/{day}/"
    
    
    try:
        response = s3_client.list_objects_v2(
            Bucket = bucket_name,
            Prefix = folder_to_delete
            )
            
        if 'Contents' in response:
            files = [{'Key': obj['Key']} for obj in reponse['Contents']]
            
            if files:
                delete_response = s3_client.delete_objects(
                        Bucket= bucket_name,
                        Delete={'Object': archivos}
                    )
            else:
                print("No hay archivos en la carpeta")
        
        else:
            print("No se encontro la carpeta")
        
    except Exeption as e:
        print(f"El error es {e}")
        raise e
    
    return {
        'statusCode': 200,
        'body': f"Carpetas eliminadas son las sigueintes {folder_to_delete}"
    }
