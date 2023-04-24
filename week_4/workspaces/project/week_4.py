from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock


@asset(required_resource_keys={'s3'},
    config_schema={'s3_key': str},
    # out={'stock_data': Out(dagster_type=List[Stock], description='list of Stock objects')},
    op_tags={'kind': 's3'},
    description='Reads stock data from an S3 bucket and returns of list of Stock objects.')
def get_s3_data(context):
    s3_key = context.op_config['s3_key']
    data = [*context.resources.s3.get_data(s3_key)]
    stocks = list(map(lambda x: Stock.from_list(x), data))
    return stocks


@asset(#ins={'stock_data': In(dagster_type=List, description='list of Stock objects')},
    # out={'high_date': Out(dagster_type=Aggregation, description='date with greatest high value')},
    description='Receives list of Stock objects, selects date with the highest daily high value, returns an Aggregation object using the date and high value.')
def process_data(get_s3_data):
    high_date = max(get_s3_data, key=lambda x: x.high)
    return Aggregation(date=high_date.date, high=high_date.high)


@asset(required_resource_keys={'redis'},
    # ins={'high_date': In(dagster_type=Aggregation, description='aggregation written to redis')},
    op_tags={'kind': 'redis'},
    description='Writes Aggregation date and high values to Redis.')
def put_redis_data(context, process_data):
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


@asset(required_resource_keys={'s3'},
    # ins={'high_date': In(dagster_type=Aggregation, description='aggregation uploaded to S3')},
    op_tags={'kind': 's3'},
    description='Writes Aggregation to S3.')
def put_s3_data(context, process_data):
    key = f'aggr_{process_data.date.strftime("%Y%m%d")}'
    context.resources.s3.put_data(key, process_data)


project_assets = load_assets_from_current_module()


machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
    config={"ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_1.csv"}}}},
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
