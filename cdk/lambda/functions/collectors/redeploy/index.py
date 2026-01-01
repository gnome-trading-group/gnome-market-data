import os
import boto3
from db import DynamoDBClient
from utils import lambda_handler, get_region_config
from constants import Status

@lambda_handler
def handler(body):
    """
    Force redeployment of all active collectors or a specific collector to pick up new task definition version.
    This should be called after updating the collectorOrchestratorVersion in config.
    """
    deployment_version = os.environ['COLLECTOR_DEPLOYMENT_VERSION']

    target_listing_id = body.get('listingId')

    db = DynamoDBClient()

    if target_listing_id:
        # Redeploy specific collector
        target_listing_id = int(target_listing_id)
        collector = db.get_item(target_listing_id)

        if not collector:
            raise Exception(f'Collector with listing ID {target_listing_id} not found')

        if collector.get('status') != Status.ACTIVE.value:
            raise Exception(f'Collector {target_listing_id} is not active (status: {collector.get("status")})')

        collectors_to_redeploy = [collector]
        operation_type = f'single collector {target_listing_id}'
    else:
        # Redeploy all active collectors
        collectors = db.get_all_items()
        collectors_to_redeploy = [c for c in collectors if c.get('status') == Status.ACTIVE.value]
        operation_type = 'all active collectors'

    results = []
    errors = []

    # Cache ECS clients by region
    ecs_clients = {}

    for collector in collectors_to_redeploy:
        listing_id = collector['listingId']
        service_name = f'collector-{listing_id}'

        region = collector.get('region')
        if not region:
            errors.append({
                'listingId': listing_id,
                'error': 'Collector does not have a region set.'
            })
            continue

        region_config = get_region_config(region)
        if not region_config:
            errors.append({
                'listingId': listing_id,
                'error': f'Region {region} is not configured'
            })
            continue

        cluster = region_config['clusterName']
        base_task_definition = region_config['taskDefinitionFamily']

        if region not in ecs_clients:
            ecs_clients[region] = boto3.client('ecs', region_name=region)
        ecs = ecs_clients[region]

        try:
            # Get the base task definition to create a new collector-specific version
            base_task_def_response = ecs.describe_task_definition(taskDefinition=base_task_definition)
            base_task_def = base_task_def_response['taskDefinition']

            # Create a new task definition for this specific collector with LISTING env var
            container_def = base_task_def['containerDefinitions'][0].copy()

            # Add LISTING to environment variables
            if 'environment' not in container_def:
                container_def['environment'] = []

            # Remove existing LISTING if present
            container_def['environment'] = [
                env for env in container_def['environment']
                if env['name'] != 'LISTING'
            ]

            # Add the LISTING for this collector
            container_def['environment'].append({
                'name': 'LISTING',
                'value': str(listing_id)
            })

            # Register a new task definition for this collector
            collector_task_def_response = ecs.register_task_definition(
                family=f'collector-{listing_id}',
                taskRoleArn=base_task_def['taskRoleArn'],
                executionRoleArn=base_task_def['executionRoleArn'],
                networkMode=base_task_def['networkMode'],
                containerDefinitions=[container_def],
                requiresCompatibilities=base_task_def['requiresCompatibilities'],
                cpu=base_task_def['cpu'],
                memory=base_task_def['memory']
            )

            collector_task_definition = collector_task_def_response['taskDefinition']['taskDefinitionArn']

            # Force new deployment with the updated task definition
            response = ecs.update_service(
                cluster=cluster,
                service=service_name,
                taskDefinition=collector_task_definition,
                forceNewDeployment=True
            )

            # Update deployment version in DynamoDB
            db.update_service(listing_id, collector['serviceArn'], deployment_version, region, Status.ACTIVE)

            results.append({
                'listingId': listing_id,
                'serviceName': service_name,
                'region': region,
                'status': 'redeployed',
                'deploymentVersion': deployment_version
            })

        except Exception as e:
            error_msg = f'Failed to redeploy collector {listing_id}: {str(e)}'
            errors.append({
                'listingId': listing_id,
                'region': region,
                'error': error_msg,
            })

    return {
        'message': f'Redeployment initiated for {operation_type} ({len(results)} collectors) with deployment version {deployment_version}',
        'deploymentVersion': deployment_version,
        'redeployed': results,
        'errors': errors,
        'totalActive': len(collectors_to_redeploy),
        'successCount': len(results),
        'errorCount': len(errors)
    }

