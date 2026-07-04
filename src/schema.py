"""
Deliverable 1 (part of): Schema definition + validation used by the Kafka
producer/consumer to guarantee only well-formed events enter the pipeline.
"""
from jsonschema import validate, ValidationError

TICKET_SCHEMA = {
    "type": "object",
    "required": ["ticket_id", "customer", "subject", "body", "category", "created_at"],
    "properties": {
        "ticket_id": {"type": "string", "pattern": "^T-[A-Za-z0-9]+$"},
        "customer": {"type": "string", "minLength": 1},
        "subject": {"type": "string", "minLength": 1},
        "body": {"type": "string", "minLength": 1},
        "category": {"type": "string", "enum": ["account", "billing", "technical", "other"]},
        "created_at": {"type": "string", "format": "date-time"},
    },
    "additionalProperties": False,
}


def validate_ticket(payload: dict) -> tuple[bool, str | None]:
    """Returns (is_valid, error_message)."""
    try:
        validate(instance=payload, schema=TICKET_SCHEMA)
        return True, None
    except ValidationError as e:
        return False, str(e.message)