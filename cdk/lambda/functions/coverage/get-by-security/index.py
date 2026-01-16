import os
import boto3
import pyarrow.parquet as pq
import io
from utils import lambda_handler
from collections import defaultdict

@lambda_handler
def handler(securityId: int):
    """
    Get coverage for a specific security across all exchanges, showing data availability by date.
    Reads from the most recent S3 inventory Parquet file.
    
    Args:
        securityId: The security ID to query
    
    Returns:
        {
            'securityId': int,
            'exchanges': {
                'exchangeId': {
                    'exchangeId': int,
                    'totalDays': int,
                    'totalMinutes': int,
                    'totalSizeBytes': int,
                    'earliestDate': str,
                    'latestDate': str,
                    'schemaTypes': [str]
                }
            },
            'coverage': {
                'YYYY-MM-DD': {
                    'hasCoverage': bool,
                    'totalMinutes': int,
                    'exchanges': {
                        'exchangeId': {
                            'minutes': int,
                            'schemaTypes': {
                                'schemaType': {
                                    'minutes': int,
                                    'sizeBytes': int
                                }
                            }
                        }
                    }
                }
            },
            'summary': {
                'totalExchanges': int,
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
                'exchanges': {},
                'coverage': {},
                'summary': {
                    'totalExchanges': 0,
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
        # Filter for this specific security
        prefix = f'{securityId}/'
        
        # Track coverage by exchange and date
        exchanges_data = defaultdict(lambda: {
            'minutes': set(),
            'sizeBytes': 0,
            'dates': set(),
            'schemaTypes': set()
        })
        
        coverage_by_date = defaultdict(lambda: {
            'minutes': set(),
            'exchanges': defaultdict(lambda: {
                'minutes': set(),
                'schemaTypes': defaultdict(lambda: {'minutes': set(), 'sizeBytes': 0})
            })
        })
        
        total_size = 0
        all_schema_types = set()
        
        # Process inventory records for this security
        for _, row in df.iterrows():
            key = row['Key']
            size = row['Size']
            
            # Only process files for this security
            if not key.startswith(prefix):
                continue
            
            # Parse key: {securityId}/{exchangeId}/{year}/{month}/{day}/{hour}/{minute}/{schemaType}.zst
            parts = key.split('/')
            if len(parts) != 8:
                continue
            
            exchange_id = parts[1]
            year = parts[2]
            month = parts[3].zfill(2)
            day = parts[4].zfill(2)
            hour = parts[5].zfill(2)
            minute = parts[6].zfill(2)
            schema_type = parts[7].replace('.zst', '')

            date_str = f'{year}-{month}-{day}'
            minute_key = f'{hour}:{minute}'

            # Track exchange-level data
            exchanges_data[exchange_id]['minutes'].add(f'{date_str}T{minute_key}')
            exchanges_data[exchange_id]['sizeBytes'] += size
            exchanges_data[exchange_id]['dates'].add(date_str)
            exchanges_data[exchange_id]['schemaTypes'].add(schema_type)

            # Track date-level coverage
            coverage_by_date[date_str]['minutes'].add(minute_key)
            coverage_by_date[date_str]['exchanges'][exchange_id]['minutes'].add(minute_key)
            coverage_by_date[date_str]['exchanges'][exchange_id]['schemaTypes'][schema_type]['minutes'].add(minute_key)
            coverage_by_date[date_str]['exchanges'][exchange_id]['schemaTypes'][schema_type]['sizeBytes'] += size

            total_size += size
            all_schema_types.add(schema_type)

        # Build exchanges summary
        exchanges_result = {}
        for exchange_id, data in exchanges_data.items():
            dates = sorted(list(data['dates']))
            exchanges_result[exchange_id] = {
                'exchangeId': int(exchange_id),
                'totalDays': len(data['dates']),
                'totalMinutes': len(data['minutes']),
                'totalSizeBytes': data['sizeBytes'],
                'earliestDate': dates[0] if dates else None,
                'latestDate': dates[-1] if dates else None,
                'schemaTypes': sorted(list(data['schemaTypes']))
            }

        # Build coverage result
        coverage_result = {}
        total_minutes = 0
        all_dates = set()

        for date_str, data in coverage_by_date.items():
            exchanges_coverage = {}

            for exchange_id, exchange_data in data['exchanges'].items():
                schema_coverage = {}
                for schema_type, schema_data in exchange_data['schemaTypes'].items():
                    schema_coverage[schema_type] = {
                        'minutes': len(schema_data['minutes']),
                        'sizeBytes': schema_data['sizeBytes']
                    }

                exchanges_coverage[exchange_id] = {
                    'minutes': len(exchange_data['minutes']),
                    'schemaTypes': schema_coverage
                }

            date_minutes = len(data['minutes'])
            total_minutes += date_minutes
            all_dates.add(date_str)

            coverage_result[date_str] = {
                'hasCoverage': True,
                'totalMinutes': date_minutes,
                'exchanges': exchanges_coverage
            }

        sorted_dates = sorted(list(all_dates))

        return {
            'securityId': securityId,
            'exchanges': exchanges_result,
            'coverage': coverage_result,
            'summary': {
                'totalExchanges': len(exchanges_data),
                'totalDays': len(all_dates),
                'totalMinutes': total_minutes,
                'earliestDate': sorted_dates[0] if sorted_dates else None,
                'latestDate': sorted_dates[-1] if sorted_dates else None,
                'totalSizeBytes': total_size,
                'schemaTypes': sorted(list(all_schema_types))
            }
        }

    except Exception as e:
        return {
            'error': str(e),
            'securityId': securityId,
            'exchanges': {},
            'coverage': {},
            'summary': {
                'totalExchanges': 0,
                'totalDays': 0,
                'totalMinutes': 0,
                'totalSizeBytes': 0
            }
        }

