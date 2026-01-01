import boto3
from db import DynamoDBClient
from utils import create_response, get_region_config

def handler(event, context):
    try:
        listing_id = int(event['pathParameters']['listingId'])

        db = DynamoDBClient()
        collector = db.get_item(listing_id)

        if not collector:
            raise Exception(f'Collector with listing ID {listing_id} not found')

        task_details = []
        if collector.get('taskArns'):
            region = collector.get('region')
            if region:
                region_config = get_region_config(region)
                if region_config:
                    cluster = region_config['clusterName']
                    ecs = boto3.client('ecs', region_name=region)

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

        return create_response(200, result)

    except Exception as e:
        return create_response(400, {'error': str(e)})
