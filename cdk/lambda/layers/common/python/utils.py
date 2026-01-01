import json
import os
import functools
from typing import Any, Callable, Dict, Optional
from decimal import Decimal
import datetime

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


def get_collector_regions() -> Dict[str, Dict[str, Any]]:
    regions_json = os.environ.get('COLLECTOR_REGIONS', '{}')
    return json.loads(regions_json)


def get_region_config(region: str) -> Optional[Dict[str, Any]]:
    regions = get_collector_regions()
    return regions.get(region)


def get_available_regions() -> list:
    return list(get_collector_regions().keys())

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS'
}

def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    """Create a standardized API response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': CORS_HEADERS,
        'body': json.dumps(body, cls=DecimalEncoder)
    }

def lambda_handler(func: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Callable[[Dict[str, Any], Any], Dict[str, Any]]:
    @functools.wraps(func)
    def wrapper(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        try:
            body = json.loads(event.get('body') or '{}')
            result = func(body)
            return create_response(200, result)
        except Exception as e:
            return create_response(400, {'error': str(e)})
    return wrapper 