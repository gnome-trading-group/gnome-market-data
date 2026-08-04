import os
import boto3
from db import DynamoDBClient
from utils import lambda_handler, get_region_config, get_available_regions

@lambda_handler
def handler(listingIds: list, region: str = None, cpu: str = None, memory: str = None):
    if not listingIds:
        raise ValueError('listingIds is required and must be non-empty')

    available_regions = get_available_regions()
    if not region:
        raise ValueError(f'region is required. Available regions: {available_regions}')
    if region not in available_regions:
        raise ValueError(f'Invalid region: {region}. Available regions: {available_regions}')

    region_config = get_region_config(region)
    cluster = region_config['clusterName']
    base_task_definition = region_config['taskDefinitionFamily']
    security_group_id = region_config['securityGroupId']
    subnet_ids = region_config['subnetIds']
    deployment_version = os.environ.get('COLLECTOR_DEPLOYMENT_VERSION', 'unknown')

    listing_ids = [int(lid) for lid in listingIds]
    first_listing_id = listing_ids[0]
    service_name = f'collector-{first_listing_id}'

    ecs = boto3.client('ecs', region_name=region)
    db = DynamoDBClient()

    base_task_def_response = ecs.describe_task_definition(taskDefinition=base_task_definition)
    base_task_def = base_task_def_response['taskDefinition']

    container_def = base_task_def['containerDefinitions'][0].copy()

    if 'environment' not in container_def:
        container_def['environment'] = []
    container_def['environment'].append({
        'name': 'LISTINGS',
        'value': ','.join(str(lid) for lid in listing_ids)
    })

    task_cpu = cpu or base_task_def['cpu']
    task_memory = memory or base_task_def['memory']

    collector_task_def_response = ecs.register_task_definition(
        family=f'collector-{first_listing_id}',
        taskRoleArn=base_task_def['taskRoleArn'],
        executionRoleArn=base_task_def['executionRoleArn'],
        networkMode=base_task_def['networkMode'],
        containerDefinitions=[container_def],
        requiresCompatibilities=base_task_def['requiresCompatibilities'],
        cpu=task_cpu,
        memory=task_memory
    )

    collector_task_definition = collector_task_def_response['taskDefinition']['taskDefinitionArn']

    service_exists = False
    try:
        describe_response = ecs.describe_services(
            cluster=cluster,
            services=[service_name]
        )
        if describe_response['services'] and describe_response['services'][0]['status'] != 'INACTIVE':
            service_exists = True
    except ecs.exceptions.ClientError:
        pass

    if service_exists:
        response = ecs.update_service(
            cluster=cluster,
            service=service_name,
            taskDefinition=collector_task_definition,
            desiredCount=2,
            forceNewDeployment=True,
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': subnet_ids,
                    'securityGroups': [security_group_id],
                    'assignPublicIp': 'ENABLED'
                }
            }
        )
        message = 'Collector service updated and redeployed'
    else:
        response = ecs.create_service(
            cluster=cluster,
            serviceName=service_name,
            taskDefinition=collector_task_definition,
            desiredCount=2,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': subnet_ids,
                    'securityGroups': [security_group_id],
                    'assignPublicIp': 'ENABLED'
                }
            },
            deploymentConfiguration={
                'maximumPercent': 200,
                'minimumHealthyPercent': 50,
                'deploymentCircuitBreaker': {
                    'enable': True,
                    'rollback': True
                }
            },
            enableExecuteCommand=True,
            propagateTags='SERVICE',
            tags=[
                {'key': 'ListingId', 'value': str(first_listing_id)},
                {'key': 'DeploymentVersion', 'value': deployment_version},
                {'key': 'Region', 'value': region}
            ]
        )
        message = 'Collector service created successfully'

    service_arn = response['service']['serviceArn']

    db.put_item(first_listing_id, service_arn, deployment_version, region,
                listing_ids=listing_ids, cpu=task_cpu, memory=task_memory)

    return {
        'message': message,
        'serviceArn': service_arn,
        'serviceName': service_name,
        'region': region,
        'listingIds': listing_ids,
        'cpu': task_cpu,
        'memory': task_memory,
        'desiredCount': 2,
        'deploymentVersion': deployment_version,
        'updated': service_exists
    }