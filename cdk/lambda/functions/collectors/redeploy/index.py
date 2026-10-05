import json
import boto3
from db import DynamoDBClient
from utils import lambda_handler, get_region_config, resolve_orchestrator_version, set_container_env
from constants import Status

@lambda_handler
def handler(listingId: int = None, orchestratorVersion: str = None):
    """
    Redeploy all active collectors, or one, onto an orchestrator version: the one given, else the latest release.
    """
    deployment_version = resolve_orchestrator_version(orchestratorVersion)

    db = DynamoDBClient()

    if listingId:
        # Redeploy specific collector
        collector = db.get_item(listingId)

        if not collector:
            raise Exception(f'Collector with listing ID {listingId} not found')

        if collector.get('status') != Status.ACTIVE.value:
            raise Exception(f'Collector {listingId} is not active (status: {collector.get("status")})')

        collectors_to_redeploy = [collector]
        operation_type = f'single collector {listingId}'
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

            listing_ids = collector['listingIds']
            task_cpu = collector.get('cpu') or base_task_def['cpu']
            task_memory = collector.get('memory') or base_task_def['memory']

            container_def = base_task_def['containerDefinitions'][0].copy()

            set_container_env(container_def, 'LISTINGS', json.dumps(listing_ids))
            set_container_env(container_def, 'ORCHESTRATOR_VERSION', deployment_version)

            collector_task_def_response = ecs.register_task_definition(
                family=f'collector-{listing_id}',
                taskRoleArn=base_task_def['taskRoleArn'],
                executionRoleArn=base_task_def['executionRoleArn'],
                networkMode=base_task_def['networkMode'],
                containerDefinitions=[container_def],
                requiresCompatibilities=base_task_def['requiresCompatibilities'],
                cpu=task_cpu,
                memory=task_memory
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
            db.update_service(listing_id, collector['serviceArn'], deployment_version, region, Status.ACTIVE,
                              listing_ids=listing_ids, cpu=task_cpu, memory=task_memory)

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

