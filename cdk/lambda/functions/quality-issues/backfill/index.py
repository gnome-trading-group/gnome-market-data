import json
import os
from datetime import date
import boto3
from utils import lambda_handler


@lambda_handler
def handler(
    exchangeId: int,
    securityId: int,
    startDate: str,
    endDate: str,
    includeStatistical: bool = False,
    resetStatistics: bool = False,
):
    try:
        start = date.fromisoformat(startDate)
        end = date.fromisoformat(endDate)
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {exc}") from exc

    if start > end:
        raise ValueError(f"startDate {startDate} must not be after endDate {endDate}")

    function_name = os.environ['QUALITY_BACKFILL_FUNCTION_NAME']

    payload = {
        'exchangeId': exchangeId,
        'securityId': securityId,
        'startDate': startDate,
        'endDate': endDate,
        'includeStatistical': includeStatistical,
        'resetStatistics': resetStatistics,
    }

    boto3.client('lambda').invoke(
        FunctionName=function_name,
        InvocationType='Event',
        Payload=json.dumps(payload),
    )

    return {
        'message': 'Backfill triggered successfully',
        'payload': payload,
    }
