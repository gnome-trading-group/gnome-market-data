import json
import os
import boto3
from utils import lambda_handler


@lambda_handler
def handler(listingId: int, timestamp: int, schemaType: str, windowMinutes: int = 30):
    function_name = os.environ['INVESTIGATION_FUNCTION_NAME']
    client = boto3.client('lambda')

    payload = {
        'listingId': listingId,
        'timestamp': timestamp,
        'schemaType': schemaType,
        'windowMinutes': windowMinutes,
    }

    response = client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload),
    )

    result_payload = response['Payload'].read()

    if response.get('FunctionError'):
        error_detail = json.loads(result_payload)
        raise RuntimeError(error_detail.get('errorMessage', 'Investigation Lambda failed'))

    return json.loads(result_payload)
