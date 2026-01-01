import json
import boto3
from typing import Dict, Any, List
from db import DynamoDBClient, Status

def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    print(f"Received event: {json.dumps(event)}")

    detail = event.get('detail', {})
    task_arn = detail.get('taskArn', '')
    last_status = detail.get('lastStatus', '')
    desired_status = detail.get('desiredStatus', '')
    stopped_reason = detail.get('stoppedReason', '')
    group = detail.get('group', '')  # Format: "service:collector-{listing_id}"

    # Extract listing_id from service name
    listing_id = None
    if group and group.startswith('service:collector-'):
        try:
            listing_id = int(group.split('collector-')[1])
        except (IndexError, ValueError):
            print(f"Could not extract listing_id from group: {group}")
            return

    if not listing_id:
        print(f"No listing_id found in event, skipping")
        return

    db = DynamoDBClient()
    collector = db.get_item(listing_id)

    if not collector:
        print(f"No collector found with listing ID {listing_id}")
        return

    # Get current task ARNs for this collector
    current_task_arns = collector.get('taskArns', [])

    # Update task ARNs list based on task status
    if last_status == 'RUNNING':
        # Add task to list if not already there
        if task_arn not in current_task_arns:
            current_task_arns.append(task_arn)
            db.update_task_arns(listing_id, current_task_arns)

        # If we have at least one running task and desired status is not STOPPED, mark as ACTIVE
        if desired_status != 'STOPPED' and collector.get('status') != Status.INACTIVE.value:
            db.update_status(listing_id, Status.ACTIVE)

    elif last_status == 'STOPPED':
        # Remove task from list
        if task_arn in current_task_arns:
            current_task_arns.remove(task_arn)
            db.update_task_arns(listing_id, current_task_arns)

        # If no tasks are running and collector is not intentionally inactive, mark as FAILED
        if len(current_task_arns) == 0 and collector.get('status') != Status.INACTIVE.value:
            db.update_status(listing_id, Status.FAILED, stopped_reason)

    elif last_status == 'PENDING':
        # Mark as pending if not already active
        if collector.get('status') not in [Status.ACTIVE.value, Status.INACTIVE.value]:
            db.update_status(listing_id, Status.PENDING)

    print(f"Updated collector {listing_id}: status={last_status}, task_arns={current_task_arns}")
