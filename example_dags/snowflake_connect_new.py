#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example use of Snowflake related operators.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_de_connection"
SNOWFLAKE_SAMPLE_TABLE = "second_json_table"

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SAMPLE_TABLE} (data VARIANT, id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} select parse_json('{{name:\"name-%(id)s\", id: %(id)s}}'), %(id)s"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "snowflake_connect_new"


with DAG(
    DAG_ID,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_snowflake]
    snowflake_op_sql_str = SnowflakeOperator(task_id="snowflake_op_sql_str", sql=CREATE_TABLE_SQL_STRING)

    snowflake_op_with_params = SnowflakeOperator(
        task_id="snowflake_op_with_params",
        sql=SQL_INSERT_STATEMENT,
        default_args={"retries": 2},
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        parameters={"id": 400},
    )

    snowflake_op_sql_list = SnowflakeOperator(task_id="snowflake_op_sql_list", sql=SQL_LIST, snowflake_conn_id=SNOWFLAKE_CONN_ID)

    snowflake_op_sql_multiple_stmts = SnowflakeOperator(
        task_id="snowflake_op_sql_multiple_stmts",
        sql=SQL_MULTIPLE_STMTS,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        split_statements=True,
    )

    (
        snowflake_op_sql_str
        >> [
            snowflake_op_with_params,
            snowflake_op_sql_list,
            snowflake_op_sql_multiple_stmts,
        ]
    )