import json
import os
from datetime import date, timedelta
import boto3
from utils import lambda_handler


@lambda_handler
def handler(
    exchangeId: int,
    securityId: int,
    startDate: str,
    endDate: str,
    mode: str = 'all',
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
    client = boto3.client('lambda')

    current = start
    days = 0
    while current <= end:
        payload = {
            'exchangeId': exchangeId,
            'securityId': securityId,
            'date': current.isoformat(),
            'mode': mode,
            'resetStatistics': resetStatistics,
        }
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(payload),
        )
        current += timedelta(days=1)
        days += 1

    return {
        'message': f'Backfill triggered for {days} day(s)',
        'days': days,
    }
