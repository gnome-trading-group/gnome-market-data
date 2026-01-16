import os
import boto3
import pyarrow.parquet as pq
import io
from utils import lambda_handler
from collections import defaultdict

@lambda_handler
def handler(securityId: int, exchangeId: int):
    """
    Get coverage for a specific security+exchange combination, showing data availability by date.
    Reads from the most recent S3 inventory Parquet file.

    Args:
        securityId: The security ID to query
        exchangeId: The exchange ID to query

    Returns:
        {
            'securityId': int,
            'exchangeId': int,
            'coverage': {
                'YYYY-MM-DD': {
                    'hasCoverage': bool,
                    'totalMinutes': int,
                    'schemaTypes': {
                        'schemaType': {
                            'hasCoverage': bool,
                            'minutes': int,
                            'sizeBytes': int
                        }
                    }
                }
            },
            'summary': {
                'totalDays': int,
                'totalMinutes': int,
                'earliestDate': str,
                'latestDate': str,
                'totalSizeBytes': int,
                'schemaTypes': [str]
            }
        }
    """
    metadata_bucket = os.environ['METADATA_BUCKET_NAME']
    s3_client = boto3.client('s3')

    inventory_prefix = 'market-data-inventory/'

    try:
        # Find the most recent inventory parquet file
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=metadata_bucket, Prefix=inventory_prefix)

        parquet_files = []
        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.parquet'):
                    parquet_files.append({
                        'key': key,
                        'last_modified': obj['LastModified']
                    })

        if not parquet_files:
            return {
                'error': 'No inventory data found. S3 inventory may not be configured or has not run yet.',
                'securityId': securityId,
                'exchangeId': exchangeId,
                'coverage': {},
                'summary': {
                    'totalDays': 0,
                    'totalMinutes': 0,
                    'totalSizeBytes': 0
                }
            }

        latest_file = sorted(parquet_files, key=lambda x: x['last_modified'], reverse=True)[0]

        response = s3_client.get_object(Bucket=metadata_bucket, Key=latest_file['key'])
        parquet_data = response['Body'].read()

        table = pq.read_table(io.BytesIO(parquet_data))
        df = table.to_pandas()

        # Key format: {securityId}/{exchangeId}/{year}/{month}/{day}/{hour}/{minute}/{schemaType}.zst
        # Filter for this specific security+exchange combination
        prefix = f'{securityId}/{exchangeId}/'

        # Track coverage by date and schema type
        coverage_by_date = defaultdict(lambda: {
            'minutes': set(),
            'schemaTypes': defaultdict(lambda: {'minutes': set(), 'sizeBytes': 0})
        })

        total_size = 0
        all_schema_types = set()

        # Process inventory records for this security+exchange
        for _, row in df.iterrows():
            key = row['Key']
            size = row['Size']

            # Only process files for this security+exchange
            if not key.startswith(prefix):
                continue

            # Parse key: {securityId}/{exchangeId}/{year}/{month}/{day}/{hour}/{minute}/{schemaType}.zst
            parts = key.split('/')
            if len(parts) != 8:
                continue

            year = parts[2]
            month = parts[3].zfill(2)
            day = parts[4].zfill(2)
            hour = parts[5].zfill(2)
            minute = parts[6].zfill(2)
            schema_type = parts[7].replace('.zst', '')

            date_str = f'{year}-{month}-{day}'
            minute_key = f'{hour}:{minute}'

            # Track coverage
            coverage_by_date[date_str]['minutes'].add(minute_key)
            coverage_by_date[date_str]['schemaTypes'][schema_type]['minutes'].add(minute_key)
            coverage_by_date[date_str]['schemaTypes'][schema_type]['sizeBytes'] += size
            total_size += size
            all_schema_types.add(schema_type)

        # Build coverage result
        coverage_result = {}
        total_minutes = 0

        for date_str, data in coverage_by_date.items():
            schema_coverage = {}
            for schema_type, schema_data in data['schemaTypes'].items():
                schema_coverage[schema_type] = {
                    'hasCoverage': True,
                    'minutes': len(schema_data['minutes']),
                    'sizeBytes': schema_data['sizeBytes']
                }

            date_minutes = len(data['minutes'])
            total_minutes += date_minutes

            coverage_result[date_str] = {
                'hasCoverage': True,
                'totalMinutes': date_minutes,
                'schemaTypes': schema_coverage
            }

        total_days = len(coverage_by_date)

        return {
            'securityId': securityId,
            'exchangeId': exchangeId,
            'coverage': coverage_result,
            'summary': {
                'totalDays': total_days,
                'totalMinutes': total_minutes,
                'earliestDate': min(coverage_by_date.keys()) if coverage_by_date else None,
                'latestDate': max(coverage_by_date.keys()) if coverage_by_date else None,
                'totalSizeBytes': total_size,
                'schemaTypes': sorted(list(all_schema_types))
            }
        }

    except Exception as e:
        return {
            'error': str(e),
            'securityId': securityId,
            'exchangeId': exchangeId,
            'coverage': {},
            'summary': {
                'totalDays': 0,
                'totalMinutes': 0,
                'totalSizeBytes': 0
            }
        }

