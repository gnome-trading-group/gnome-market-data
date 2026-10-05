from db import DynamoDBClient
from utils import lambda_handler, get_regional_client, get_region_config

db = DynamoDBClient()

@lambda_handler
def handler(listingId: int):
    collector = db.get_item(listingId)

    if not collector:
        raise Exception(f'Collector with listing ID {listingId} not found')

    task_details = []
    if collector.get('taskArns'):
        region = collector.get('region')
        if region:
            region_config = get_region_config(region)
            if region_config:
                cluster = region_config['clusterName']
                ecs = get_regional_client('ecs', region)

                try:
                    response = ecs.describe_tasks(
                        cluster=cluster,
                        tasks=collector['taskArns']
                    )
                    task_details = response.get('tasks', [])
                except Exception as e:
                    print(f"Error fetching task details: {e}")

    result = {
        'collector': collector,
        'taskDetails': task_details
    }

    return result
