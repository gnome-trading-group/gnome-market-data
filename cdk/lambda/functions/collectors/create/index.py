import os
import boto3
from db import DynamoDBClient
from utils import lambda_handler, get_region_config, get_available_regions

@lambda_handler
def handler(body):
    listing_id = int(body['listingId'])
    region = body.get('region')

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

    ecs = boto3.client('ecs', region_name=region)

    service_name = f'collector-{listing_id}'

    try:
        db = DynamoDBClient()

        base_task_def_response = ecs.describe_task_definition(taskDefinition=base_task_definition)
        base_task_def = base_task_def_response['taskDefinition']

        container_def = base_task_def['containerDefinitions'][0].copy()

        if 'environment' not in container_def:
            container_def['environment'] = []
        container_def['environment'].append({
            'name': 'LISTING',
            'value': str(listing_id)
        })

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
                    {'key': 'ListingId', 'value': str(listing_id)},
                    {'key': 'DeploymentVersion', 'value': deployment_version},
                    {'key': 'Region', 'value': region}
                ]
            )
            message = 'Collector service created successfully'

        service_arn = response['service']['serviceArn']

        db.put_item(listing_id, service_arn, deployment_version, region)

        return {
            'message': message,
            'serviceArn': service_arn,
            'serviceName': service_name,
            'region': region,
            'desiredCount': 2,
            'deploymentVersion': deployment_version,
            'updated': service_exists
        }

    except Exception as e:
        raise Exception(f'Failed to create/update collector service: {str(e)}')