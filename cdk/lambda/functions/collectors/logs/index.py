import time
from utils import lambda_handler, get_regional_client, get_region_config, create_response
from db import DynamoDBClient

db = DynamoDBClient()

# Covers the shutdown output a task writes around the time ECS reports it stopped.
STOPPED_TASK_LOGS_GRACE_MS = 5 * 60 * 1000

@lambda_handler
def handler(listingId: int):
    collector = db.get_item(listingId)

    if not collector:
        raise Exception(f'Collector with listing ID {listingId} not found')

    region = collector.get('region')
    if not region:
        raise Exception(f'Collector {listingId} does not have a region set.')

    region_config = get_region_config(region)
    if not region_config:
        raise Exception(f'Region {region} is not configured')

    log_group_name = region_config['logGroupName']

    logs_client = get_regional_client('logs', region)

    now = int(time.time() * 1000)
    task_arns = collector.get('taskArns', [])
    # Once nothing runs, the logs shown are those of the collector's last run, ending where it stopped.
    if task_arns:
        tasks = [(task_arn, now) for task_arn in task_arns]
    else:
        tasks = [
            (t['taskArn'], min(int(t['stoppedAt']) + STOPPED_TASK_LOGS_GRACE_MS, now))
            for t in collector.get('stoppedTasks', [])
        ]
    if not tasks:
        return {'logs': []}

    logs = []

    for task_arn, end_time in tasks:
        task_id = task_arn.split('/')[-1]
        log_stream_prefix = f"collector/CollectorContainer/{task_id}"

        try:
            log_events = []

            try:
                # With no startTime the stream's newest events are returned, however long ago they were written.
                events_response = logs_client.get_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_prefix,
                    endTime=end_time,
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

    return {'logs': logs}
