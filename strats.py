import time
import multiprocessing as mp
from gpu_runner import run_strat
from cpu_runner import Runner
from data_fetcher import DataFetcher
import tqdm
import argparse
from graph_generator import gen_csv, show_graph
from numba import cuda
import numpy as np
import math
import psutil
from decimal import Decimal
import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="TQQQ", help="Symbol")
    parser.add_argument(
        "--src", default="aws", help="Data source", choices={"yfinance", "aws"}
    )
    parser.add_argument(
        "--days", default=20, type=int, help="Number of days to compute"
    )
    parser.add_argument("--start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("--end", default=16, type=float, help="End of day time")
    parser.add_argument("--buy", default=10, type=int, help="Buy max percent to test")
    parser.add_argument("--sell", default=10, type=int, help="Sell max percent to test")
    parser.add_argument(
        "--brange", default=0, type=int, help="Buy max percent range to test"
    )
    parser.add_argument(
        "--srange", default=0, type=int, help="Sell max percent range to test"
    )
    parser.add_argument(
        "--include-partial-today", action="store_true", help="Skip todays info"
    )
    parser.add_argument("--cpu", action="store_true", help="Use CPU instead of gpu")
    args = parser.parse_args()

    data_cache = DataFetcher()

    days = data_cache.poll_data(
        args.src,
        args.symbol,
        args.start,
        args.end,
        args.days,
        args.include_partial_today,
    )
    if args.cpu:
        datapoints = get_starmap_datapoints(
            data_cache, args.buy, args.sell, args.brange, args.srange
        )
        memory = get_memory_usage()
        results, start_time = run_strats_multiprocess(datapoints)
    else:
        datapoints = get_datapoints(args.buy, args.sell, args.brange, args.srange)
        memory = get_memory_usage()
        results, start_time = run_strats_gpu(datapoints, data_cache)
    analyse_results(results, args.symbol, start_time, days, args.src, memory)


def get_starmap_datapoints(data_cache, buy, sell, brange, srange):
    print("generating starmap datapoints")
    datapoints = [
        (data_cache, b / 10, -(s / 10), br / 10, sr / 10)
        for b in range(buy * (-10), buy * 10 + 1)
        for s in range(sell * (-10), sell * 10 + 1)
        for br in range(0, brange * 10 + 1)
        for sr in range(0, srange * 10 + 1)
    ]
    print(f"number of datapoints: {len(datapoints)}")
    return datapoints


def get_datapoints(buy, sell, brange, srange):
    print("generating datapoints")

    def get_index(buy, sell, brange, srange):
        return (20 * buy + 1) * (20 * sell + 1) * (brange * 10 + 1) * (srange * 10 + 1)

    array = np.empty(shape=(get_index(buy, sell, brange, srange), 7), dtype=float)
    i = 0
    for b in range(buy * (-10), buy * 10 + 1):
        for s in range(sell * (-10), sell * 10 + 1):
            for br in range(0, brange * 10 + 1):
                for sr in range(0, srange * 10 + 1):
                    array[i] = np.array([b / 10, -(s / 10), br / 10, sr / 10, 0, 0, 0])
                    i = i + 1
    print(f"number of datapoints: {len(array)}")
    return array


def persist_results(
    symbol,
    highest_profit,
    highest_profit_b,
    highest_profit_s,
    transactions,
    invested_time,
    start_time,
    rbm,
    rsm,
    memory,
    max_val,
    key_val,
    computed_days,
    data_source,
):
    info_str = f"for {symbol} highest profit {highest_profit} when buying at {highest_profit_b} and selling at {highest_profit_s} with {transactions} transactions and invested time {invested_time} in seconds and elapsed time {time.time() - start_time} to calculate and range buy {rbm} and range sell {rsm} with memory {memory}. Multiple day cumulative profit: max {max_val} for buy at {key_val.split('_')[0]} and sell at {key_val.split('_')[1]} with rangeB {key_val.split('_')[2]} and rangeS {key_val.split('_')[3]} with {len(computed_days)} number of days"
    print(info_str)
    info = {
        "info": info_str,
        "symbol": symbol,
        "profit": highest_profit,
        "buy": highest_profit_b,
        "sell": highest_profit_s,
        "transactions": transactions,
        "invsted_time": invested_time,
        "elapsed_time": time.time() - start_time,
        "range_buy": rbm,
        "range_sell": rbm,
        "days_nb": len(computed_days),
        "days": [d.strftime("%Y-%m-%d") for d in computed_days],
        "data_source": data_source,
        "memory": memory,
        "multiday_profit": max_val,
        "multiday_buy": key_val.split("_")[0],
        "multiday_sell": key_val.split("_")[1],
        "multiday_rangeB": key_val.split("_")[2],
        "multiday_rangeS": key_val.split("_")[3],
    }
    for key, val in info.items():
        if isinstance(val, int) or isinstance(val, float):
            info[key] = Decimal(val)
    with open(f"log.txt", "a") as file:
        file.write(f"{info_str}\n")
    dyn_resource = boto3.resource("dynamodb")
    table = dyn_resource.Table("Results")
    table.put_item(Item=info)


def get_memory_usage():
    print("RAM Used (GB):", psutil.virtual_memory()[3] / 1000000000)
    return psutil.virtual_memory()[3] / 1000000000


def analyse_results(results, symbol, start_time, computed_days, data_source, memory):
    compiled = {}
    highest_profit = -100000
    highest_profit_b = highest_profit_s = transactions = invested_time = rbm = rsm = 0

    for result in results:
        buy, sell, rb, rs, profit, transactionsnb, itime = result.ravel()

        cumul = compiled.setdefault(f"{buy}_{sell}_{rb}_{rs}", 0) + profit
        compiled.update({f"{buy}_{sell}_{rb}_{rs}": cumul})

        if profit > highest_profit:
            highest_profit = profit
            highest_profit_b = buy
            highest_profit_s = sell
            transactions = transactionsnb
            invested_time = itime
            rbm = rb
            rsm = rs

    max_val = 0
    key_val = "0_0_0_0"
    for k in compiled.keys():
        if compiled[k] > max_val:
            max_val = compiled[k]
            key_val = k
    persist_results(
        symbol,
        highest_profit,
        highest_profit_b,
        highest_profit_s,
        transactions,
        invested_time,
        start_time,
        rbm,
        rsm,
        memory,
        max_val,
        key_val,
        computed_days,
        data_source,
    )
    # gen_csv(compiled)
    # show_graph(compiled)


def run_strats_gpu(datapoints, data_cache):
    print("running on gpu")
    start_time = time.time()
    data = data_cache.data
    threadsperblock = 1024
    blockspergrid = math.ceil(datapoints.shape[0] / threadsperblock)

    np_data = np.array(data)
    run_strat[blockspergrid, threadsperblock](datapoints, np_data)

    return datapoints, start_time


def run_strats_multiprocess(datapoints):
    print("running on cpu")
    start_time = time.time()
    p = mp.Pool()
    results = p.starmap(run_single, tqdm.tqdm(datapoints, total=len(datapoints)))
    p.close()
    return results, start_time


def run_single(
    cache,
    buy_on_diff_percent,
    sell_on_diff_percent,
    range_b,
    range_s,
    symbol="TQQQ",
    day_range=2,
):
    data = cache.data
    strat_runner = Runner(data, 100000)
    profit, transactions, total_time_invested = strat_runner.run_strat(
        buy_on_diff_percent - (range_b / 2),
        buy_on_diff_percent + (range_b / 2),
        sell_on_diff_percent - (range_s / 2),
        sell_on_diff_percent + (range_s / 2),
    )
    return np.array(
        [
            buy_on_diff_percent,
            sell_on_diff_percent,
            range_b,
            range_s,
            profit,
            transactions,
            total_time_invested,
        ]
    )


if __name__ == "__main__":
    main()
