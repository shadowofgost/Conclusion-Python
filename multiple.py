"""
# @Author           : Albert Wang
# @Copyright Notice : Copyright (c) 2022 Albert Wang 王子睿, All Rights Reserved.
# @Time             : 2022-02-19 17:52:27
# @Description      :
# @Email            : shadowofgost@outlook.com
# @FilePath         : /Office/multiple.py
# @LastAuthor       : Albert Wang
# @LastTime         : 2023-03-30 15:50:24
# @Software         : Vscode
"""
import sys

sys.path.append(".")
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
from time import time
from typing import List

from numpy import array, dtype, float64, int64, zeros, isnan, NAN
from pandas import DataFrame
from pmdarima import auto_arima
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from DataClean import multi_update
from DataBase import BitCoinData, GoldData, get_session, session

import ray


def arima_prediction(data: List[int]) -> List[Decimal]:
    """
    arima_prediction _summary_

    _extended_summary_

    Parameters
    ----------
    data : List[Decimal]
        _description_

    Returns
    -------
    List[Decimal]
        _description_
    """
    try:
        model = auto_arima(
            data,
            max_p=7,
            max_q=7,
            seasonal=False,
            max_d=5,
            trace=False,
            information_criterion="bic",
            error_action="ignore",
            suppress_warnings=True,
            stepwise=False,
        )
        forecast = model.predict(1)
        return [Decimal.from_float((forecast[0]) / 100)]
    except Exception as e:
        return [Decimal.from_float(data[-1] / 100), Decimal.from_float(data[-2] / 100)]


def circle_prediction(bitcoin_data, gold_data, length: int):
    """
    circle_trade _summary_

    _extended_summary_

    Parameters
    ----------
    data : _type_
        _description_
    """
    id_recent = 1  ##最近的非null的id
    for i in range(len(bitcoin_data)):
        if bitcoin_data[i, 2] == 0:
            pass
        else:
            id_recent = bitcoin_data[i, 1]
        if i == 0 or i == 1 or i == 2:
            gold_prices = [gold_data[id_recent - 1, 2], 0]
            bitcoin_prices = [bitcoin_data[i, 0], 0]
        else:
            if i >= length:
                bitcoin_predict_data = bitcoin_data[i - length : i, 0]
            else:
                bitcoin_predict_data = bitcoin_data[:i, 0]
            ##获取传入预测黄金的数据
            if id_recent >= length:
                gold_predict_data = gold_data[id_recent - length : id_recent, 2]
            else:
                gold_predict_data = gold_data[:id_recent, 2]
            if bitcoin_data[i, 2] == 0:
                gold_prices = [gold_data[id_recent - 1, 2], 0]
                bitcoin_prices = arima_prediction(bitcoin_predict_data)
            else:
                gold_prices = arima_prediction(gold_predict_data)
                bitcoin_prices = arima_prediction(bitcoin_predict_data)
        bitcoin_predict_price = bitcoin_prices[0]
        gold_predict_price = gold_prices[0]
        bitcoin_data[i, 4] = bitcoin_predict_price
        gold_data[id_recent - 1, 1] = gold_predict_price
    print(str(length) + "数据预测完成")
    return bitcoin_data, gold_data


def single_arima_prediction(i: dict) -> dict:
    tmp = {}
    tmp["id"] = i["id"]
    ##print(i["id"])
    tmp["id_recent"] = i["id_recent"]
    if i["type"] == 1:
        gold_prices = i["gold_predict_data"]
        bitcoin_prices = arima_prediction(i["bitcoin_predict_data"])
    elif i["type"] == 2:
        gold_prices = arima_prediction(i["gold_predict_data"])
        bitcoin_prices = arima_prediction(i["bitcoin_predict_data"])
    else:
        gold_prices = i["gold_predict_data"]
        bitcoin_prices = i["bitcoin_predict_data"]
    tmp["gold_predict_price"] = gold_prices[0]
    tmp["bitcoin_predict_price"] = bitcoin_prices[0]
    return tmp


def multi_arima_prediction_process(args: list):
    """
    multi_arima_prediction_process _summary_

    _extended_summary_

    Parameters
    ----------
    args : list
        _description_

    Returns
    -------
    _type_
        _description_
    """
    result_data = []
    for i in args:
        tmp = single_arima_prediction(i)
        result_data.append(tmp)
    return result_data


def multiprocessing_process(multiple_data_list):
    multiple_numbers = 12
    process_args = []
    length_per_process = int(len(multiple_data_list) / multiple_numbers)
    for i in range(multiple_numbers):
        if i == multiple_numbers - 1:
            process_args.append(multiple_data_list[i * length_per_process :])
        else:
            process_args.append(
                multiple_data_list[
                    i * length_per_process : (i + 1) * length_per_process
                ]
            )
    pool = ProcessPoolExecutor(multiple_numbers)
    result_list = pool.map(multi_arima_prediction_process, process_args)
    result_data = []
    for j in result_list:
        result_data.extend(j)
    return result_data


@ray.remote
def ray_arima_prediction(i: dict) -> dict:
    tmp = {}
    tmp["id"] = i["id"]
    ##print(i["id"])
    tmp["id_recent"] = i["id_recent"]
    if i["type"] == 1:
        gold_prices = i["gold_predict_data"]
        bitcoin_prices = arima_prediction(i["bitcoin_predict_data"])
    elif i["type"] == 2:
        gold_prices = arima_prediction(i["gold_predict_data"])
        bitcoin_prices = arima_prediction(i["bitcoin_predict_data"])
    else:
        gold_prices = i["gold_predict_data"]
        bitcoin_prices = i["bitcoin_predict_data"]
    tmp["gold_predict_price"] = gold_prices[0]
    tmp["bitcoin_predict_price"] = bitcoin_prices[0]
    return tmp


def ray_process(operation, multiple_data_list):
    result_data = ray.get([operation.remote(i) for i in multiple_data_list])
    return result_data


def multiprocess_circle_prediction(bitcoin_data, gold_data, length: int):
    """
    circle_trade _summary_

    _extended_summary_

    Parameters
    ----------
    data : _type_
        _description_
    """
    start_time = time()
    multiple_data_list = []
    id_recent = 1  ##最近的非null的id
    for i in range(len(bitcoin_data)):
        tmp = {}
        tmp["id"] = i
        if bitcoin_data[i, 2] == 0:
            pass
        else:
            id_recent = bitcoin_data[i, 1]
        if i == 0 or i == 1 or i == 2:
            gold_prices = [gold_data[id_recent - 1, 2], 0]
            bitcoin_prices = [bitcoin_data[i, 0], 0]
            tmp["gold_predict_data"] = gold_prices
            tmp["bitcoin_predict_data"] = bitcoin_prices
            tmp["type"] = 0
        else:
            if i >= length:
                bitcoin_predict_data = bitcoin_data[i - length : i, 0]
            else:
                bitcoin_predict_data = bitcoin_data[:i, 0]
            ##获取传入预测黄金的数据
            if id_recent >= length:
                gold_predict_data = gold_data[id_recent - length : id_recent, 2]
            else:
                gold_predict_data = gold_data[:id_recent, 2]
            if bitcoin_data[i, 2] == 0:
                gold_prices = [gold_data[id_recent - 1, 2], 0]
                tmp["gold_predict_data"] = gold_prices
                tmp["bitcoin_predict_data"] = bitcoin_predict_data
                tmp["type"] = 1
            else:
                tmp["gold_predict_data"] = gold_predict_data
                tmp["bitcoin_predict_data"] = bitcoin_predict_data
                tmp["type"] = 2
        tmp["id_recent"] = id_recent
        multiple_data_list.append(tmp)
    start_process_time = time()
    # result_data = multiprocessing_process(multiple_data_list)
    result_data = ray_process(ray_arima_prediction, multiple_data_list)
    end_process_time = time()
    for i in result_data:
        id = i["id"]
        id_recent = i["id_recent"]
        bitcoin_data[id, 4] = i["bitcoin_predict_price"]
        gold_data[id_recent - 1, 1] = i["gold_predict_price"]
    print(str(length) + "数据预测完成")
    end_time = time()
    print("预处理时间：" + str(start_process_time - start_time))
    print("并行化时间：" + str(end_process_time - start_process_time))
    print("转换结果时间" + str(end_time - end_process_time))
    return bitcoin_data, gold_data


def get_data(session: Session):
    """
    get_initial_data _summary_

    _extended_summary_

    Parameters
    ----------
    session : Session
        _description_
    """
    start_time = time()
    stmt = (
        select(
            BitCoinData.id,  # type: ignore
            BitCoinData.value.label("bitcoin_value"),
            GoldData.id.label("gold_id"),  # type: ignore
            GoldData.value.label("gold_value"),
            BitCoinData.predict_value,
        )
        .join(
            GoldData,
            and_(
                BitCoinData.year == GoldData.year,
                BitCoinData.month == GoldData.month,
                BitCoinData.day == GoldData.day,
            ),
            isouter=True,
        )
        .order_by(BitCoinData.id)  # type: ignore
    )
    bitcoin_initial_data = session.execute(stmt).mappings().all()
    df = DataFrame(bitcoin_initial_data)
    bitcoin_initial_data = df.to_numpy(dtype=dtype(float64))
    stmt = select(GoldData.id, GoldData.value, GoldData.predict_value).order_by(  # type: ignore
        GoldData.id
    )
    gold_initial_data = session.execute(stmt).mappings().all()
    df = DataFrame(gold_initial_data)
    gold_initial_data = df.to_numpy(dtype=dtype(float64))
    bitcoin_data = zeros((len(bitcoin_initial_data), 5), dtype=int64)
    gold_data = zeros((len(gold_initial_data), 3), dtype=int64)
    for i in range(len(gold_initial_data)):
        gold_data[i, 0] = int(gold_initial_data[i, 0])
        gold_data[i, 1] = int(gold_initial_data[i, 1] * 100)
        gold_data[i, 2] = int(gold_initial_data[i, 2] * 100)
    for i in range(len(bitcoin_initial_data)):
        bitcoin_data[i, 0] = int(bitcoin_initial_data[i, 0] * 100)
        bitcoin_data[i, 3] = int(bitcoin_initial_data[i, 3])
        bitcoin_data[i, 4] = int(bitcoin_initial_data[i, 4] * 100)
        if isnan(bitcoin_initial_data[i, 1]):
            bitcoin_data[i, 1] = 0
            bitcoin_data[i, 2] = 0
        else:
            bitcoin_data[i, 1] = int(bitcoin_initial_data[i, 1])
            bitcoin_data[i, 2] = int(bitcoin_initial_data[i, 2] * 100)
    end_time = time()
    print("数据获取已经完成, 时长：", end_time - start_time)
    return bitcoin_data, gold_data


def update_database(session: Session, bitcoin_data, gold_data):
    """
    update_database _summary_

    _extended_summary_

    Parameters
    ----------
    session : Session
        _description_
    """
    gold_data_mappings = [
        {
            "id": gold_data[i, 0],
            "predict_value": gold_data[i, 1],
        }
        for i in range(len(gold_data))
    ]
    bitcoin_data_mappings = [
        {
            "id": bitcoin_data[i, 3],
            "predict_value": bitcoin_data[i, 4],
        }
        for i in range(len(bitcoin_data))
    ]
    multi_update(GoldData, gold_data_mappings, session)
    multi_update(BitCoinData, bitcoin_data_mappings, session)
    print("数据库更新完成")


def fix_predict_value(initial_data):
    """
    fix_predict_value _summary_

    _extended_summary_

    Parameters
    ----------
    initial_data : _type_
        _description_
    """
    ##print(gold_initial_data, bitcoin_initial_data)
    for i in range(len(initial_data)):
        predict_data = initial_data[i, 1]
        history_data = initial_data[i, 2]
        if i == len(initial_data) - 1:
            if predict_data == 0:
                initial_data[i, 1] = history_data
        else:
            if predict_data == 0:
                if initial_data[i - 1, 1] == 0:
                    initial_data[i, 1] = (initial_data[i + 1, 1] + history_data) / 2
                elif initial_data[i + 1, 1] == 0:
                    initial_data[i, 1] = (initial_data[i - 1, 1] + history_data) / 2
                elif initial_data[i + 1, 1] == 0 and initial_data[i - 1, 1] == 0:
                    initial_data[i, 1] = history_data
                else:
                    initial_data[i, 1] = (
                        initial_data[i - 1, 1] + history_data + initial_data[i + 1, 1]
                    ) / 3
    return initial_data


def fix_fix_predict_value(initial_data):
    """
    fix_predict_value _summary_

    _extended_summary_

    Parameters
    ----------
    initial_data : _type_
        _description_
    """
    ##print(gold_initial_data, bitcoin_initial_data)
    for i in range(len(initial_data)):
        predict_data = initial_data[i, 1]
        history_data = initial_data[i, 2]
        if i == len(initial_data) - 1:
            if predict_data == 0:
                initial_data[i, 1] = history_data
        else:
            if predict_data == history_data:
                initial_data[i, 1] = (
                    initial_data[i - 1, 1] + initial_data[i + 1, 1]
                ) / 2
    return initial_data


def fix_predict_error(session: Session):
    """
    fix_predict_error _summary_

    _extended_summary_

    Parameters
    ----------
    session : Session
        _description_
    """
    gold_sql = select(GoldData.id, GoldData.value, GoldData.predict_value).order_by(  # type: ignore
        GoldData.id
    )
    bitcoin_sql = select(
        BitCoinData.id, BitCoinData.value, BitCoinData.predict_value  # type: ignore
    ).order_by(BitCoinData.id)
    bitcoin_initial_data = session.execute(bitcoin_sql).mappings().all()
    df = DataFrame(bitcoin_initial_data)
    bitcoin_initial_data = df.to_numpy(dtype=dtype(Decimal))
    gold_initial_data = session.execute(gold_sql).mappings().all()
    df = DataFrame(gold_initial_data)
    gold_initial_data = df.to_numpy(dtype=dtype(Decimal))
    result_data = fix_fix_predict_value(gold_initial_data)
    gold_data_mappings = [
        {
            "id": result_data[i, 0],
            "predict_value": result_data[i, 1],
        }
        for i in range(len(result_data))
    ]
    multi_update(GoldData, gold_data_mappings, session)
    result_data = fix_fix_predict_value(bitcoin_initial_data)
    bitcoin_data_mappings = [
        {
            "id": result_data[i, 0],
            "predict_value": result_data[i, 1],
        }
        for i in range(len(result_data))
    ]
    multi_update(BitCoinData, bitcoin_data_mappings, session)
    print("预测值修复完成")


def main():
    """
    main _summary_

    _extended_summary_
    """
    list = [7, 30, 90, 180, 360]
    session_list, engine_list = get_session()
    for i in range(len(session_list)):
        bitcoin_initial_data, gold_initial_data = get_data(session_list[i])
        bitcoin_data, gold_data = circle_prediction(
            bitcoin_initial_data, gold_initial_data, list[i]
        )
        update_database(session_list[i], bitcoin_data, gold_data)
        fix_predict_error(session_list[i])
    print("success")


def test():
    """
    test _summary_

    _extended_summary_
    """
    list = [7, 30, 90, 180, 360]
    session_list, engine_list = get_session()
    bitcoin_initial_data, gold_initial_data = get_data(session_list[0])

    bitcoin_data, gold_data = multiprocess_circle_prediction(
        bitcoin_initial_data, gold_initial_data, list[0]
    )
    # update_database(session_list[0], bitcoin_data, gold_data)
    print("测试成功")


if __name__ == "__main__":
    ## main()
    ray.init()
    start_time = time()
    test()
    end_time = time()
    print("程序运行时间：", end_time - start_time)
