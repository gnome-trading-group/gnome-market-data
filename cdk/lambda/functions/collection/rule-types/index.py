from collection_rules import RULE_TYPES
from utils import lambda_handler


@lambda_handler
def handler():
    """Each rule type's parameter schema, for the controller's rule form."""
    return {'ruleTypes': RULE_TYPES}
