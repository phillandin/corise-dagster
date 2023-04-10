import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
    DynamicOut,
    DynamicOutput,
    Output
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={'s3_key': str})
def get_s3_data_op(context):
    s3_key = context.op_config['s3_key']
    stocks = [i for i in csv_helper(s3_key)]
    return stocks


@op(ins={'stock_data': In(dagster_type=List, description='list of Stock objects')},
    out={'high_date': Out(dagster_type=Aggregation, description='date with greatest high value')})
def process_data_op(stock_data):
    high_date = max(stock_data, key=lambda x: x.high)
    return Aggregation(date=high_date.date, high=high_date.high)


@op(ins={'high_date': In(dagster_type=Aggregation)})
def put_redis_data_op(high_date):
    pass


@op(ins={'high_date': In(dagster_type=Aggregation)})
def put_s3_data_op(high_date):
    pass


@job
def machine_learning_job():
    stock_data_list = get_s3_data_op()
    high = process_data_op(stock_data_list)
    put_redis_data_op(high)
    put_s3_data_op(high)
