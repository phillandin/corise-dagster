from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(required_resource_keys={'s3'},
    config_schema={'s3_key': str},
    out={'stock_data': Out(dagster_type=List[Stock], description='list of Stock objects')})
def get_s3_data(context):
    s3_key = context.op_config['s3_key']
    data = [*context.resources.s3.get_data(s3_key)]
    stocks = list(map(lambda x: Stock.from_list(x), data))
    return stocks


@op(ins={'stock_data': In(dagster_type=List, description='list of Stock objects')},
    out={'high_date': Out(dagster_type=Aggregation, description='date with greatest high value')})
def process_data(stock_data):
    high_date = max(stock_data, key=lambda x: x.high)
    return Aggregation(date=high_date.date, high=high_date.high)


@op(required_resource_keys={'redis'},
    ins={'high_date': In(dagster_type=Aggregation, description='date with greatest high value')})
def put_redis_data(context, high_date):
    context.resources.redis.put_data(str(high_date.date), str(high_date.high))


@op(required_resource_keys={'s3'},
    ins={'high_date': In(dagster_type=Aggregation, description='date with greatest high value')})
def put_s3_data(context, high_date):
    s3_key = S3_FILE
    context.resources.s3.put_data(s3_key, high_date)


@graph
def machine_learning_graph():
    stock_data_list = get_s3_data()
    high = process_data(stock_data_list)
    put_redis_data(high)
    put_s3_data(high)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
)
