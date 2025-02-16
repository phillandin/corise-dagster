from random import randint

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    ResourceDefinition,
    String,
    graph,
    op,
)
from dagster_dbt import dbt_cli_resource, dbt_run_op, dbt_test_op
from workspaces.config import ANALYTICS_TABLE, DBT, POSTGRES
from workspaces.resources import postgres_resource


@op(
    config_schema={"table_name": String},
    out=Out(String),
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_dbt_table(context: OpExecutionContext):
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    ins={"table_name": In(dagster_type=String)},
    out=Out(Nothing),
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def insert_dbt_data(context: OpExecutionContext, table_name: String):
    sql = f"INSERT INTO {table_name} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 100)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@op(
    required_resource_keys={"dbt"},
    ins={"start_after": In(Nothing)},
    out={
        "success": Out(is_required=False),
        "failure": Out(is_required=False),
    },
    tags={"kind": "dbt"},
)
def dbt_test_op_conditional(context: OpExecutionContext):
    result = context.resources.dbt.test()
    if result.return_code == 0:
        yield Output(None, "success")
    else:
        yield Output(None, "failure")


@op(
    ins={"start_after": In(Nothing)},
    out=Out(Nothing),
)
def success_dbt_op(context: OpExecutionContext):
    context.log.info("Successful dbt run!")


@op(
    ins={"start_after": In(Nothing)},
    out=Out(Nothing),
)
def failure_dbt_op(context: OpExecutionContext):
    context.log.info("Issue with the dbt run!")


@graph
def dbt_graph():
    table = create_dbt_table()
    inserted_data = insert_dbt_data(table)
    dbt_run = dbt_run_op(inserted_data)
    success, failure = dbt_test_op_conditional(dbt_run)
    success_dbt_op(success)
    failure_dbt_op(failure)


local = {
    "ops": {"create_dbt_table": {"config": {"table_name": ANALYTICS_TABLE}}},
}


docker = {
    "resources": {
        "database": {"config": POSTGRES},
        "dbt": {"config": DBT},
    },
    "ops": {"create_dbt_table": {"config": {"table_name": ANALYTICS_TABLE}}},
}

# This actually won't run with the ResourceDefinition.mock_resource() for dbt because the output will not meet the contract 
dbt_job_local = dbt_graph.to_job(
    name="week_2_challenge_local",
    config=docker,
    resource_defs={
        "database": ResourceDefinition.mock_resource(),
        "dbt": ResourceDefinition.mock_resource(),
    },
)


dbt_job_docker = dbt_graph.to_job(
    name="week_2_challenge_docker",
    config=docker,
    resource_defs={
        "database": postgres_resource,
        "dbt": dbt_cli_resource,
    },
)
