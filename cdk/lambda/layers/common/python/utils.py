import json
import os
import functools
import inspect
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

def lambda_handler(func: Callable) -> Callable[[Dict[str, Any], Any], Dict[str, Any]]:
    """
    API Gateway Lambda handler decorator that automatically extracts function parameters from the event.

    The decorator inspects the function signature and automatically populates parameters from:
    1. event['body'] (parsed JSON for POST/PUT/DELETE requests)
    2. event['pathParameters'] (URL path parameters)
    3. event['queryStringParameters'] (URL query parameters)
    4. event (the full event dict, if parameter name is 'event')
    5. context (the Lambda context, if parameter name is 'context')

    Parameters are matched by name and populated in order of precedence:
    - pathParameters (highest priority)
    - queryStringParameters
    - body (lowest priority)

    Example usage:
        @lambda_handler
        def handler(listingId: int, region: str = None):
            # listingId and region are automatically extracted from event
            return {'listingId': listingId, 'region': region}

    Automatically wraps the response with proper status codes and CORS headers.
    """
    @functools.wraps(func)
    def wrapper(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        try:
            if event.get('body') is None:
                body = {}
            else:
                body = json.loads(event.get('body'))

            path_params = event.get('pathParameters') or {}
            query_params = event.get('queryStringParameters') or {}

            sig = inspect.signature(func)

            kwargs = {}

            for param_name, param in sig.parameters.items():
                if param_name == 'event':
                    kwargs['event'] = event
                    continue
                elif param_name == 'context':
                    kwargs['context'] = context
                    continue

                value = None
                if param_name in path_params:
                    value = path_params[param_name]
                elif param_name in query_params:
                    value = query_params[param_name]
                elif param_name in body:
                    value = body[param_name]

                if value is None:
                    if param.default == inspect.Parameter.empty:
                        raise ValueError(f"Required parameter '{param_name}' not found in request")
                    continue

                if param.annotation != inspect.Parameter.empty:
                    try:
                        if param.annotation == int:
                            value = int(value)
                        elif param.annotation == float:
                            value = float(value)
                        elif param.annotation == bool:
                            value = value if isinstance(value, bool) else value.lower() in ('true', '1', 'yes')
                        elif param.annotation == str:
                            value = str(value)
                    except (ValueError, AttributeError) as e:
                        raise ValueError(f"Cannot convert parameter '{param_name}' to {param.annotation.__name__}: {e}")

                kwargs[param_name] = value

            result = func(**kwargs)

            if isinstance(result, dict) and 'statusCode' in result:
                return result

            return create_response(200, result)
        except Exception as e:
            return create_response(400, {'error': str(e)})

    return wrapper