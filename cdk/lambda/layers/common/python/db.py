import boto3
import os
from typing import Dict, List, Optional
import time
from constants import Status

class DynamoDBClient:
    def __init__(self):
        self.table_name = os.environ['COLLECTORS_TABLE_NAME']
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_name)

    def get_all_items(self) -> List[Dict]:
        response = self.table.scan()
        return response.get('Items', [])

    def get_item(self, listing_id: int) -> Optional[Dict]:
        response = self.table.get_item(Key={'listingId': listing_id})
        return response.get('Item')

    def put_item(self, listing_id: int, serviceArn: str, deploymentVersion: str, region: str) -> Dict:
        """Create or update a collector with service-based deployment info"""
        existing_item = self.get_item(listing_id)

        if existing_item:
            return self.update_service(listing_id, serviceArn, deploymentVersion, region, Status.PENDING)
        else:
            return self.table.put_item(
                Item={
                    'listingId': listing_id,
                    'status': Status.PENDING.value,
                    'serviceArn': serviceArn,
                    'deploymentVersion': deploymentVersion,
                    'region': region,
                    'taskArns': [],  # Will be populated by ECS monitor
                    'lastStatusChange': int(time.time()),
                    'failureReason': None,
                }
            )

    def update_service(self, listing_id: int, serviceArn: str, deploymentVersion: str, region: str, status: Status) -> Dict:
        """Update service ARN, deployment version, and region"""
        return self.table.update_item(
            Key={'listingId': listing_id},
            UpdateExpression='SET serviceArn = :serviceArn, deploymentVersion = :version, #r = :region, #s = :status, lastStatusChange = :now',
            ExpressionAttributeValues={
                ':serviceArn': serviceArn,
                ':version': deploymentVersion,
                ':region': region,
                ':status': status.value,
                ':now': int(time.time())
            },
            ExpressionAttributeNames={
                '#s': 'status',
                '#r': 'region'
            }
        )

    def update_status(self, listing_id: int, status: Status, failureReason: Optional[str] = None) -> Dict:
        """Update collector status"""
        return self.table.update_item(
            Key={'listingId': listing_id},
            UpdateExpression='SET #s = :status, lastStatusChange = :now, failureReason = :reason',
            ExpressionAttributeValues={
                ':status': status.value,
                ':now': int(time.time()),
                ':reason': failureReason
            },
            ExpressionAttributeNames={
                '#s': 'status'
            }
        )

    def update_task_arns(self, listing_id: int, task_arns: List[str]) -> Dict:
        """Update the list of running task ARNs for a collector"""
        return self.table.update_item(
            Key={'listingId': listing_id},
            UpdateExpression='SET taskArns = :taskArns',
            ExpressionAttributeValues={
                ':taskArns': task_arns
            }
        )

    def update_heartbeat(self, listing_id: int) -> Dict:
        return self.table.update_item(
            Key={'listingId': listing_id},
            UpdateExpression='SET lastHeartbeat = :now',
            ExpressionAttributeValues={':now': int(time.time())}
        )