import time
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("datalake-dev-niif-short-links")


def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    token = params.get("t") or params.get("token")

    if not token:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "text/plain; charset=utf-8"
            },
            "body": "Token requerido"
        }

    item = table.get_item(
        Key={"token": token}
    ).get("Item")

    if not item:
        return {
            "statusCode": 404,
            "headers": {
                "Content-Type": "text/plain; charset=utf-8"
            },
            "body": "Link no encontrado"
        }

    expires_at = int(item.get("expiresAt", 0))

    if expires_at and int(time.time()) > expires_at:
        return {
            "statusCode": 410,
            "headers": {
                "Content-Type": "text/plain; charset=utf-8"
            },
            "body": "Link expirado"
        }

    return {
        "statusCode": 302,
        "headers": {
            "Location": item["url"]
        },
        "body": ""
    }