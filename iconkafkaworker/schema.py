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

transactions_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "version": {"type": ["string", "null"]},
        "from_address": {"type": ["string", "null"]},
        "to_address": {"type": ["string", "null"]},
        "value": {"type": ["integer", "null"]},
        "step_limit": {"type": ["integer", "null"]},
        "timestamp": {"type": "string"},
        "block_timestamp": {"type": "integer"},
        "nid": {"type": ["integer", "null"]},
        "nonce": {"type": ["integer", "null"]},
        "hash": {"type": "string"},
        "transaction_index": {"type": "integer"},
        "block_hash": {"type": "string"},
        "block_number": {"type": "integer"},
        "fee": {"type": ["integer", "null"]},
        "signature": {"type": ["string", "null"]},
        "data_type": {"type": ["string", "null"]},
        "data": {"type": ["object", "string", "null"]},
        "receipt_cumulative_step_used": {
            "type": ["integer", "null"],
        },
        "receipt_step_used": {"type": ["integer", "null"]},
        "receipt_step_price": {"type": ["integer", "null"]},
        "receipt_score_address": {"type": ["string", "null"]},
        "receipt_logs": {"type": ["string", "null"]},
        "receipt_status": {"type": "integer"},
        "item_id": {"type": "string"},
        "item_timestamp": {"type": "string"},
    },
}

registrations_schema = {
    "type": "object",
    "properties": {
        "type": {"type": ["string", "null"]},
        "from_address": {"type": ["string", "null"]},
        "to_address": {"type": ["string", "null"]},
        "value": {"type": ["number", "null"]},
        "keyword": {"type": ["string", "null"]},
        "position": {"type": ["integer", "null"]},
    },
}


def get_logs_schema(topic):
    schema = logs_schema
    schema["title"] = topic + "-value"
    return schema


def get_registrations_schema(topic):
    schema = registrations_schema
    schema["title"] = topic + "-value"
    return schema


def get_transactions_schema(topic):
    schema = transactions_schema
    schema["title"] = topic + "-value"
    return schema
