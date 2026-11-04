import json

def lambda_handler(event, context):
    # TODO implement
    path = event['path']
    file_name = event['file_name']

    print(event)

    text = f"esta es la ruta {path} y este es el archivo {file_name}"
    return {
        'statusCode': 200,
        'body': json.dumps(text)
    }
