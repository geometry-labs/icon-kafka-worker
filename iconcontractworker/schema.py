logs_schema = {
    "log": {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "log_index": {"type": "integer"},
            "transaction_hash": {"type": "string"},
            "transaction_index": {"type": "integer"},
            "address": {"type": "string"},
            "data": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "default": None,
            },
            "indexed": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "default": None,
            },
            "block_number": {"type": "integer"},
            "block_timestamp": {"type": "integer"},
            "block_hash": {"type": "string"},
            "item_id": {"type": "string"},
            "item_timestamp": {"type": "string"},
        },
    }
}

registrations_schema = {}


def get_logs_schema(topic):
    schema = logs_schema
    schema["title"] = topic
    return schema


def get_registrations_schema(topic):
    schema = registrations_schema
    schema["title"] = topic
    return schema
