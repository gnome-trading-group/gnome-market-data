import os
import boto3
import pyarrow.parquet as pq
import io
from utils import lambda_handler
from datetime import datetime
from collections import defaultdict

@lambda_handler
def handler():
    """
    Get coverage summary of all market data from S3 inventory.
    Reads the most recent Parquet inventory file from the metadata bucket.

    Returns:
        {
            'totalFiles': int,
            'totalSizeBytes': int,
            'totalMinutes': int,
            'securityExchangeCount': int,
            'securities': {
                '{securityId}-{exchangeId}': {
                    'securityId': int,
                    'exchangeId': int,
                    'fileCount': int,
                    'minuteCount': int,
                    'sizeBytes': int,
                    'earliestDate': str,
                    'latestDate': str,
                    'schemaTypes': [str]
                }
            },
            'schemaTypes': {
                'schemaType': {
                    'fileCount': int,
                    'minuteCount': int,
                    'sizeBytes': int
                }
            },
            'lastInventoryDate': str
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
                'totalFiles': 0,
                'totalSizeBytes': 0,
                'totalMinutes': 0,
                'securityExchangeCount': 0,
                'securities': {},
                'schemaTypes': {},
            }

        # Get the most recent parquet file
        latest_file = sorted(parquet_files, key=lambda x: x['last_modified'], reverse=True)[0]

        # Download and parse the parquet file
        response = s3_client.get_object(Bucket=metadata_bucket, Key=latest_file['key'])
        parquet_data = response['Body'].read()

        # Read parquet file
        table = pq.read_table(io.BytesIO(parquet_data))
        df = table.to_pandas()

        # Initialize aggregation structures
        total_files = 0
        total_size = 0
        total_minutes = 0
        securities_data = defaultdict(lambda: {
            'fileCount': 0,
            'minuteCount': 0,
            'sizeBytes': 0,
            'earliestDate': None,
            'latestDate': None,
            'schemaTypes': set(),
            'minutes': set()
        })
        schema_types_data = defaultdict(lambda: {'fileCount': 0, 'minuteCount': 0, 'sizeBytes': 0, 'minutes': set()})

        # Process inventory records
        # S3 inventory columns: Bucket, Key, Size, LastModifiedDate, ETag, etc.
        # Key format: {securityId}/{exchangeId}/{year}/{month}/{day}/{hour}/{minute}/{schemaType}.zst
        for _, row in df.iterrows():
            key = row['Key']
            size = row['Size']

            # Parse key
            parts = key.split('/')
            if len(parts) != 8:
                continue

            security_id = parts[0]
            exchange_id = parts[1]
            year = parts[2]
            month = parts[3].zfill(2)
            day = parts[4].zfill(2)
            hour = parts[5].zfill(2)
            minute = parts[6].zfill(2)
            schema_type = parts[7].replace('.zst', '')

            date_str = f'{year}-{month}-{day}'
            minute_key = f'{date_str}T{hour}:{minute}'
            security_key = f'{security_id}-{exchange_id}'

            total_files += 1
            total_size += size

            # Update security data
            security_data = securities_data[security_key]
            security_data['fileCount'] += 1
            security_data['sizeBytes'] += size
            security_data['schemaTypes'].add(schema_type)
            security_data['minutes'].add(minute_key)

            if security_data['earliestDate'] is None or date_str < security_data['earliestDate']:
                security_data['earliestDate'] = date_str
            if security_data['latestDate'] is None or date_str > security_data['latestDate']:
                security_data['latestDate'] = date_str

            # Update schema type data
            schema_types_data[schema_type]['fileCount'] += 1
            schema_types_data[schema_type]['sizeBytes'] += size
            schema_types_data[schema_type]['minutes'].add(minute_key)

        # Convert sets to lists/counts for JSON serialization
        securities_result = {}
        for security_key, data in securities_data.items():
            security_id, exchange_id = security_key.split('-')
            minute_count = len(data['minutes'])
            total_minutes += minute_count

            securities_result[security_key] = {
                'securityId': int(security_id),
                'exchangeId': int(exchange_id),
                'fileCount': data['fileCount'],
                'minuteCount': minute_count,
                'sizeBytes': data['sizeBytes'],
                'earliestDate': data['earliestDate'],
                'latestDate': data['latestDate'],
                'schemaTypes': sorted(list(data['schemaTypes']))
            }

        schema_types_result = {}
        for schema_type, data in schema_types_data.items():
            schema_types_result[schema_type] = {
                'fileCount': data['fileCount'],
                'minuteCount': len(data['minutes']),
                'sizeBytes': data['sizeBytes']
            }

        # Extract inventory date from the file key
        inventory_date = latest_file['last_modified'].strftime('%Y-%m-%d')

        return {
            'totalFiles': total_files,
            'totalSizeBytes': total_size,
            'totalMinutes': total_minutes,
            'securityExchangeCount': len(securities_data),
            'securities': securities_result,
            'schemaTypes': schema_types_result,
            'lastInventoryDate': inventory_date,
        }

    except Exception as e:
        return {
            'error': str(e),
            'totalFiles': 0,
            'totalSizeBytes': 0,
            'totalMinutes': 0,
            'securityExchangeCount': 0,
            'securities': {},
            'schemaTypes': {},
        }

