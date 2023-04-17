from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(required_resource_keys={'s3'},
    config_schema={'s3_key': str},
    out={'stock_data': Out(dagster_type=List[Stock], description='list of Stock objects')},
    tags={'kind': 's3'})
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
    ins={'high_date': In(dagster_type=Aggregation, description='aggregation written to redis')},
    tags={'kind': 'redis'})
def put_redis_data(context, high_date):
    context.resources.redis.put_data(str(high_date.date), str(high_date.high))


@op(required_resource_keys={'s3'},
    ins={'high_date': In(dagster_type=Aggregation, description='aggregation uploaded to S3')},
    tags={'kind': 's3'})
def put_s3_data(context, high_date):
    key = f'aggr_{high_date.date.strftime("%Y%m%d")}'
    context.resources.s3.put_data(key, high_date)


@graph
def machine_learning_graph():
    stock_data_list = get_s3_data()
    high = process_data(stock_data_list)
    put_redis_data(high)
    put_s3_data(high)
    

local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key: str):
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS},
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f'prefix/stock_{partition_key}.csv'}}},
    }


docker_retry_policy = RetryPolicy(max_retries=10, delay=1)


machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={'s3': mock_s3_resource, 'redis': ResourceDefinition.mock_resource()}
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker_config,
    resource_defs={'s3': s3_resource, 'redis': redis_resource},
    op_retry_policy=docker_retry_policy,
)


machine_learning_schedule_local = ScheduleDefinition(job=machine_learning_job_local, cron_schedule='*/15 * * * *')


@schedule(job=machine_learning_job_docker, cron_schedule='0 * * * *')
def machine_learning_schedule_docker():
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config=docker_config,
        tags={"date": scheduled_date},
    )


@sensor(job=machine_learning_job_docker, minimum_interval_seconds=30)
def machine_learning_sensor_docker():
    new_keys = get_s3_keys(bucket='dagster', prefix='prefix', endpoint_url='http://localstack:4566')
    if not new_keys:
        yield SkipReason('No new s3 files found in bucket.')
    for key in new_keys:
        yield RunRequest(
            run_key=key,
            run_config={
                "resources": {
                    "s3": {"config": S3},
                    "redis": {"config": REDIS},
                },
                'ops': {
                    'get_s3_data': {'config': {'s3_key': key}}
                }
            }
        )
