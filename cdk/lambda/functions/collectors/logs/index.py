import boto3
from datetime import datetime, timedelta
from utils import create_response, get_region_config
from db import DynamoDBClient

def handler(event, context):
    try:
        listing_id = int(event['pathParameters']['listingId'])

        db = DynamoDBClient()
        collector = db.get_item(listing_id)

        if not collector:
            raise Exception(f'Collector with listing ID {listing_id} not found')

        region = collector.get('region')
        if not region:
            raise Exception(f'Collector {listing_id} does not have a region set.')

        region_config = get_region_config(region)
        if not region_config:
            raise Exception(f'Region {region} is not configured')

        log_group_name = region_config['logGroupName']

        logs_client = boto3.client('logs', region_name=region)

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=10)

        task_arns = collector.get('taskArns', [])
        if not task_arns:
            return create_response(200, {'logs': []})

        logs = []

        for task_arn in task_arns:
            task_id = task_arn.split('/')[-1]
            log_stream_prefix = f"collector/CollectorContainer/{task_id}"

            try:
                log_events = []

                try:
                    events_response = logs_client.get_log_events(
                        logGroupName=log_group_name,
                        logStreamName=log_stream_prefix,
                        startTime=int(start_time.timestamp() * 1000),
                        endTime=int(end_time.timestamp() * 1000),
                        limit=500
                    )

                    for event in events_response.get('events', []):
                        log_events.append({
                            'timestamp': event['timestamp'],
                            'message': event['message'],
                            'logStreamName': log_stream_prefix
                        })
                except Exception as e:
                    print(f"Error fetching events from stream {log_stream_prefix}: {e}")

                for log_event in log_events:
                    if 'timestamp' in log_event and isinstance(log_event['timestamp'], datetime):
                        log_event['timestamp'] = int(log_event['timestamp'].timestamp() * 1000)

                log_events.sort(key=lambda x: x['timestamp'], reverse=True)

                console_url = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/{log_group_name.replace('/', '%2F')}/log-events/{log_stream_prefix.replace('/', '%2F')}"

                logs.append({
                    'taskArn': task_arn,
                    'logs': log_events[:500],
                    'consoleUrl': console_url,
                })

            except Exception as e:
                print(f"Error fetching logs: {e}")
                result = {
                    'logs': [],
                    'error': str(e)
                }
                return create_response(500, result)

        return create_response(200, { 'logs': logs })

    except Exception as e:
        return create_response(400, {'error': str(e)})
