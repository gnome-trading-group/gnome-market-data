import json
import os
import boto3
from utils import lambda_handler


@lambda_handler
def handler(listingId: int, startTimestamp: int, endTimestamp: int, maxPoints: int = 5000):
    function_name = os.environ['BBO_TIMELINE_FUNCTION_NAME']
    client = boto3.client('lambda')

    payload = {
        'listingId': listingId,
        'startTimestamp': startTimestamp,
        'endTimestamp': endTimestamp,
        'maxPoints': maxPoints,
    }

    response = client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload),
    )

    result_payload = response['Payload'].read()

    if response.get('FunctionError'):
        error_detail = json.loads(result_payload)
        raise RuntimeError(error_detail.get('errorMessage', 'BBO timeline Lambda failed'))

    return json.loads(result_payload)
