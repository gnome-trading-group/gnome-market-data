from collection import actor, wake_reconciler
from utils import lambda_handler


@lambda_handler
def handler(event: dict):
    """Runs the reconciler now rather than at the next minute."""
    if not wake_reconciler(f'manual:{actor(event)}'):
        raise RuntimeError('the reconciler queue refused the wake-up')
    return {'queued': True}
