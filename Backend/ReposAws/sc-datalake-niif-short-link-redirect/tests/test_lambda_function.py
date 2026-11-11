import os
import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

# Variables de entorno para tests
os.environ["SHORT_LINKS_TABLE"] = "niif-qa-short-links"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_REGION"] = "us-east-1"

# Mock boto3 antes de importar el módulo
with patch("boto3.resource") as mock_boto_resource:
    mock_dynamo = MagicMock()
    mock_boto_resource.return_value = mock_dynamo
    from short_link_redirect import lambda_handler
    import short_link_redirect


@pytest.fixture
def mock_table():
    """
    Mock de DynamoDB.Table usado dentro del lambda_handler.
    La tabla se instancia dentro del handler, por lo que se mockea
    el recurso de dynamodb a nivel de módulo.
    """
    mock_tbl = MagicMock()
    with patch.object(short_link_redirect, "dynamodb") as mock_dynamo:
        mock_dynamo.Table.return_value = mock_tbl
        yield mock_tbl


def test_missing_param_returns_400():
    """Test: sin parámetros retorna 400."""
    event = {"queryStringParameters": None}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 400


def test_empty_params_returns_400():
    """Test: parámetros vacíos retorna 400."""
    event = {"queryStringParameters": {}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 400


def test_not_found_returns_404(mock_table):
    """Test: item no encontrado retorna 404."""
    mock_table.get_item.return_value = {}
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 404


def test_missing_url_returns_400(mock_table):
    """Test: item sin URL retorna 400 (token sin URL asociada)."""
    mock_table.get_item.return_value = {
        "Item": {"token": "abc123"}
    }
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 400


def test_valid_redirect_returns_302(mock_table):
    """Test: item válido retorna redirect 302 con Location correcto."""
    mock_table.get_item.return_value = {
        "Item": {"token": "abc123", "url": "https://example.com"}
    }
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 302
    assert result["headers"]["Location"] == "https://example.com"


def test_no_expiry_returns_302(mock_table):
    """Test: item sin campo expiresAt retorna redirect 302."""
    mock_table.get_item.return_value = {
        "Item": {"token": "abc123", "url": "https://example.com"}
    }
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 302


def test_alternative_param_token(mock_table):
    """Test: parámetro alternativo 'token' funciona correctamente."""
    mock_table.get_item.return_value = {
        "Item": {"token": "xyz", "url": "https://test.com"}
    }
    event = {"queryStringParameters": {"token": "xyz"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 302


def test_url_with_html_entities_is_unescaped(mock_table):
    """Test: URLs con entidades HTML como &amp; son decodificadas correctamente."""
    mock_table.get_item.return_value = {
        "Item": {"token": "abc123", "url": "https://example.com?a=1&amp;b=2"}
    }
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 302
    assert result["headers"]["Location"] == "https://example.com?a=1&b=2"


def test_redirect_has_cache_control_header(mock_table):
    """Test: la respuesta 302 incluye el header Cache-Control: no-store."""
    mock_table.get_item.return_value = {
        "Item": {"token": "abc123", "url": "https://example.com"}
    }
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 302
    assert result["headers"].get("Cache-Control") == "no-store"


def test_redirect_has_cors_header(mock_table):
    """Test: la respuesta 302 incluye el header CORS."""
    mock_table.get_item.return_value = {
        "Item": {"token": "abc123", "url": "https://example.com"}
    }
    event = {"queryStringParameters": {"t": "abc123"}}
    result = lambda_handler(event, None)
    assert result["statusCode"] == 302
    assert result["headers"].get("Access-Control-Allow-Origin") == "*"


def test_json_default_decimal_integer():
    """Test: json_default convierte Decimal entero a int."""
    from short_link_redirect import json_default
    result = json_default(Decimal("5"))
    assert result == 5
    assert isinstance(result, int)


def test_json_default_decimal_float():
    """Test: json_default convierte Decimal con decimales a float."""
    from short_link_redirect import json_default
    result = json_default(Decimal("3.14"))
    assert result == pytest.approx(3.14)
    assert isinstance(result, float)


def test_json_default_unsupported_type_raises():
    """Test: json_default lanza TypeError para tipos no soportados."""
    from short_link_redirect import json_default
    with pytest.raises(TypeError):
        json_default(object())
