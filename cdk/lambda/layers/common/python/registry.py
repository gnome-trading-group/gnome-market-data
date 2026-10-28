"""The registry's read API, with the key its secret mirrors (gnome-shared-cdk ApiKeySecret)."""
import json
import os
import urllib.parse
import urllib.request

import boto3

_secrets = boto3.client('secretsmanager')
_api_key = None


def get(path: str, params: dict) -> list:
    url = f"{os.environ['REGISTRY_API_URL'].rstrip('/')}/{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={'x-api-key': _key()})
    with urllib.request.urlopen(request, timeout=25) as response:
        return json.loads(response.read())


def _key() -> str:
    # Read once per container.
    global _api_key
    if _api_key is None:
        _api_key = _secrets.get_secret_value(SecretId=os.environ['REGISTRY_API_KEY_SECRET'])['SecretString']
    return _api_key
