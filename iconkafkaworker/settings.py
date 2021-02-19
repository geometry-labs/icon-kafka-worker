#  Copyright 2021 Geometry Labs, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
from enum import Enum

from pydantic import BaseSettings


class Mode(Enum):
    CONTRACT = "contract"
    TRANSACTION = "transaction"


class Settings(BaseSettings):
    KAFKA_SERVER: str
    CONSUMER_GROUP: str = "contract_worker"
    SCHEMA_SERVER: str = None
    KAFKA_COMPRESSION: str = "gzip"
    KAFKA_MIN_COMMIT_COUNT: int = 10
    REGISTRATIONS_TOPIC: str = "event_registrations"
    BROADCASTER_EVENTS_TOPIC: str = "broadcaster_events"
    BROADCASTER_EVENTS_TABLE: str = "broadcaster_registrations"
    LOGS_TOPIC: str = "logs"
    TRANSACTIONS_TOPIC: str = "transactions"
    OUTPUT_TOPIC: str
    POSTGRES_SERVER: str
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DATABASE: str = "postgres"
    PROCESSING_MODE: Mode = Mode.CONTRACT

    class Config:
        env_prefix = "CONTRACT_WORKER_"
        case_sensitive = True


if os.environ.get("ENV_FILE", False):
    settings = Settings(_env_file=os.environ.get("ENV_FILE"))
else:
    settings = Settings()
