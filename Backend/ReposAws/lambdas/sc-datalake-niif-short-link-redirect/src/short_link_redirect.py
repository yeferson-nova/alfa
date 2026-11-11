import os
import json
import html
import boto3
from decimal import Decimal
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")

SHORT_LINKS_TABLE = os.getenv("SHORT_LINKS_TABLE", "niif-qa-short-links")


def json_default(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def response(status_code, body=None, headers=None):
    return {
        "statusCode": status_code,
        "headers": headers or {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": "" if body is None else (
            body if isinstance(body, str) else json.dumps(body, ensure_ascii=False, default=json_default)
        )
    }


def lambda_handler(event, context):
    print("[EVENT]", json.dumps(event, default=str))

    params = event.get("queryStringParameters") or {}
    token = params.get("t") or params.get("token")

    if not token:
        return response(400, {
            "message": "Token requerido",
            "expected": "/r?t={token}"
        })

    table = dynamodb.Table(SHORT_LINKS_TABLE)

    item = table.get_item(
        Key={
            "token": token
        }
    ).get("Item")

    if not item:
        return response(404, {
            "message": "Token no encontrado",
            "token": token
        })

    target_url = item.get("url")

    if not target_url:
        return response(400, {
            "message": "Token sin URL asociada",
            "token": token,
            "item": item
        })

    # Algunos URLs vienen desde HTML/JSON con &amp;.
    target_url = html.unescape(str(target_url))

    print("[REDIRECT]", token, "->", target_url)

    return {
        "statusCode": 302,
        "headers": {
            "Location": target_url,
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*"
        },
        "body": ""
    }
