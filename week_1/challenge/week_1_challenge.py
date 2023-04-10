import csv
from datetime import datetime
from heapq import nlargest
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type,
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


@op(
    config_schema={'s3_key': str},
    out={
            'empty_stocks': Out(is_required=False),
            'stock_data': Out(dagster_type=List[Stock], is_required=False),
    }
)
def get_s3_data_op(context):
    s3_key = context.op_config['s3_key']
    stock_data = [*csv_helper(s3_key)]
    if len(stock_data) == 0:
        yield Output(None, 'empty_stocks')
    else:
        yield Output(stock_data, 'stock_data')


@op(
    config_schema={
        'nlargest': int
    },
    ins={
        'stock_data': In(dagster_type=List, description='list of Stock objects')
    },
    out=DynamicOut()
)
def process_data_op(context, stock_data):
    nlargest = context.op_config['nlargest']
    stock_data.sort(key=lambda x: x.high)
    highest_highs = stock_data[:nlargest]
    for date in highest_highs:
        yield DynamicOutput(Aggregation(date=date.date, high=date.high), mapping_key=date.date.strftime('%Y_%m_%d'))


@op(ins={'highs_list': In(dagster_type=List[Aggregation])})
def put_redis_data_op(highs_list):
    pass


@op(ins={'highs_list': In(dagster_type=List[Aggregation])})
def put_s3_data_op(highs_list):
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    out=Out(Nothing),
    description="Notify if stock list is empty",
)
def empty_stock_notify_op(context: OpExecutionContext, empty_stocks: Any):
    context.log.info("No stocks returned")
    context.log.info(f'Received input of type {type(empty_stocks)}')


@job
def machine_learning_dynamic_job():
    empty_stocks, stock_data = get_s3_data_op()
    empty_stock_notify_op(empty_stocks)
    highs = process_data_op(stock_data).collect()
    put_redis_data_op(highs)
    put_s3_data_op(highs)
