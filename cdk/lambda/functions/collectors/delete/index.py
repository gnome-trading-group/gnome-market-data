import boto3
from db import DynamoDBClient
from utils import lambda_handler, get_region_config
from constants import Status

@lambda_handler
def handler(listingId: int):
    db = DynamoDBClient()

    collector = db.get_item(listingId)
    if not collector:
        raise Exception(f'Collector with listing ID {listingId} not found')

    region = collector.get('region')
    if not region:
        raise Exception(f'Collector {listingId} does not have a region set.')

    region_config = get_region_config(region)
    if not region_config:
        raise Exception(f'Region {region} is not configured')

    cluster = region_config['clusterName']

    ecs = boto3.client('ecs', region_name=region)

    service_name = f'collector-{listingId}'

    # Update status to inactive first
    db.update_status(listingId, Status.INACTIVE)

    # Delete the ECS service
    try:
        # First, scale down to 0
        ecs.update_service(
            cluster=cluster,
            service=service_name,
            desiredCount=0
        )

        # Then delete the service
        ecs.delete_service(
            cluster=cluster,
            service=service_name,
            force=True
        )

        return {
            'message': 'Collector service deleted successfully',
            'serviceName': service_name,
            'region': region
        }
    except ecs.exceptions.ServiceNotFoundException:
        return {
            'message': 'Collector service not found (may have been already deleted)',
            'serviceName': service_name,
            'region': region
        }
    except ecs.exceptions.ClientError as e:
        print(f'Error deleting service: {e}')
        raise Exception(f'Failed to delete collector service: {str(e)}')