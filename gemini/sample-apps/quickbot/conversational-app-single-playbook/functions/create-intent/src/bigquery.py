# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import re
from typing import List, Optional

from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    ScalarQueryParameter,
)
from src.models import Embedding

BIG_QUERY_DATASET = os.getenv("BIG_QUERY_DATASET", "")

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DATASET_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")

EMBEDDINGS_TABLE = "embeddings"
EMBEDDINGS_ID_COLUMN = "id"
EMBEDDINGS_TEXT_COLUMN = "text"
EMBEDDINGS_INDEX_COLUMN = "index"

INTENTS_TABLE = "intents"
INTENTS_TABLE_ID_COLUMN = "name"
INTENTS_TABLE_STATUS_COLUMN = "status"


class BigQueryRepository:
    """Repository for interacting with BigQuery using parameterized queries."""

    def __init__(self):
        self.client: Client = Client()

    @staticmethod
    def _validate_identifier(identifier: str) -> None:
        if not isinstance(identifier, str) or not IDENTIFIER_PATTERN.fullmatch(
            identifier
        ):
            raise ValueError(f"Invalid BigQuery identifier: {identifier!r}")

    @staticmethod
    def _validate_dataset(dataset: str) -> None:
        if dataset and not DATASET_PATTERN.fullmatch(dataset):
            raise ValueError(f"Invalid BigQuery dataset: {dataset!r}")

    def run_query(self, query: str, job_config: Optional[QueryJobConfig] = None):
        logging.info(query)
        return self.client.query(query, job_config=job_config).result()

    def get_row_by_id(self, table_id: str, id_column: str, id_value: str):
        self._validate_dataset(BIG_QUERY_DATASET)
        self._validate_identifier(table_id)
        self._validate_identifier(id_column)
        query = f"""
                SELECT * FROM `{BIG_QUERY_DATASET}.{table_id}`
                WHERE `{id_column}` = @id_value;
            """
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("id_value", "STRING", id_value),
            ]
        )
        return self.run_query(query, job_config=job_config)

    def insert_rows(self, table_id: str, embeddings: List[Embedding]):
        self._validate_dataset(BIG_QUERY_DATASET)
        self._validate_identifier(table_id)
        values = [embedding.to_dict() for embedding in embeddings]
        errors = self.client.insert_rows(
            f"{BIG_QUERY_DATASET}.{table_id}", values, Embedding.__schema__()
        )
        if errors == []:
            print("Embeddings added")
        else:
            print(f"Encountered errors while inserting rows: {errors}")
            raise Exception("Error inserting embeddings in BQ")

    def update_intent_status(self, intent_name: str, intent_status: str):
        self._validate_dataset(BIG_QUERY_DATASET)
        query = f"""
            UPDATE `{BIG_QUERY_DATASET}.{INTENTS_TABLE}`
            SET `{INTENTS_TABLE_STATUS_COLUMN}` = @intent_status
            WHERE `{INTENTS_TABLE_ID_COLUMN}` = @intent_name;
            """
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("intent_status", "STRING", intent_status),
                ScalarQueryParameter("intent_name", "STRING", intent_name),
            ]
        )
        return self.run_query(query, job_config=job_config)
