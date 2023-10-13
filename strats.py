import time
import multiprocessing as mp
from gpu_runner import run_strat
from cpu_runner import Runner
from data_fetcher import DataFetcher
import tqdm
import argparse
import numpy as np
import math
import psutil
from decimal import Decimal
import boto3
from datetime import datetime
import sys

this = sys.modules[__name__]
this.siltent_log = False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="TQQQ", help="Symbol")
    parser.add_argument(
        "--src", default="aws", help="Data source", choices={"yfinance", "aws"}
    )
    parser.add_argument(
        "--days", default=30, type=int, help="Number of days to compute"
    )
    parser.add_argument("--start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("--end", default=16, type=float, help="End of day time")
    parser.add_argument("--buy", default=10, type=float, help="Buy max percent to test")
    parser.add_argument("--sell", default=10, type=float, help="Sell max percent to test")
    parser.add_argument(
        "--brange", default=5, type=float, help="Buy max percent range to test"
    )
    parser.add_argument(
        "--srange", default=5, type=float, help="Sell max percent range to test"
    )
    parser.add_argument(
        "--include-partial-today", action="store_true", help="By default it skip todays info"
    )
    parser.add_argument("--cpu", action="store_true", help="Use CPU instead of gpu")
    parser.add_argument("--single-strat", action="store_true", help="Run single strat instead of all possibilities")
    parser.add_argument(
        "--tdelta", default=0, type=int, help="Number of days in the past from which to start pulling data"
    )
    parser.add_argument(
        "--agg", default=60, type=int, help="Number of seconds to aggregate"
    )
    args = parser.parse_args()
    data_cache = DataFetcher()
    run(data_cache, args.src, args.symbol, args.start, args.end, args.days, args.include_partial_today, args.cpu, args.buy, args.sell, args.brange, args.srange, args.single_strat, args.tdelta, args.agg)

def run(data_cache, src, symbol, start, end, days, include_partial_today, cpu, buy, sell, brange, srange, single_strat, tdelta, set_siltent_log=False, pregenerated_strategies=None, agg):
    this.siltent_log = set_siltent_log
    data_cache.poll_data(
        src,
        symbol,
        start,
        end,
        days,
        include_partial_today,
        tdelta=tdelta,
    )
    log(f"number of data points: {len(data_cache.data)}")
    memory = get_memory_usage()
    agg = int(agg)
    if cpu:
        strategies = get_starmap_strategies(
            data_cache, buy, sell, brange, srange, single_strat, agg
        )
        memory = get_memory_usage()
        results, start_time = run_strats_multiprocess(strategies)
    else:
        if pregenerated_strategies is not None:
            strategies=pregenerated_strategies
        else:
            strategies = get_strategies(buy, sell, brange, srange, single_strat, agg)
            memory = get_memory_usage()
        results, start_time = run_strats_gpu(strategies, data_cache)
    analysed = analyse_results(results=results, symbol=symbol, start_time=start_time, requested_days=data_cache.requested_days, data_source=src, memory=memory, actual_days=data_cache.actual_days, single_strat=single_strat, agg=agg)
    log("Done")
    return analysed


def get_starmap_strategies(data_cache, buy, sell, brange, srange, single_strat, agg):
    log("generating starmap strategies")
    if single_strat:
        return [[data_cache, buy, sell, brange, srange, agg]]
    strategies = [
        (data_cache, b / 10, s / 10, br / 10, sr / 10, agg)
        for b in range(buy * (-10), buy * 10 + 1)
        for s in range(sell * (-10), sell * 10 + 1)
        for br in range(0, brange * 10 + 1)
        for sr in range(0, srange * 10 + 1)
    ]
    log(strategies)
    log(f"number of strategies: {len(strategies)}")
    return strategies


def get_strategies(buy, sell, brange, srange, single_strat, agg):
    log("generating strategies")
    if single_strat:
        return np.array([[buy, sell, brange, srange, agg]])
    def get_index(buy, sell, brange, srange):
        return (20 * buy + 1) * (20 * sell + 1) * (brange * 10 + 1) * (srange * 10 + 1)

    array = np.empty(shape=(get_index(buy, sell, brange, srange), 7), dtype=float)
    i = 0
    for b in range(buy * (-10), buy * 10 + 1):
        for s in range(sell * (-10), sell * 10 + 1):
            for br in range(0, brange * 10 + 1):
                for sr in range(0, srange * 10 + 1):
                    array[i] = np.array([b / 10, -(s / 10), br / 10, sr / 10, agg, 0, 0])
                    i = i + 1
    log(f"number of strategies: {len(array)}")
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
    requested_days,
    data_source,
    max_buy,
    min_buy,
    max_sell,
    min_sell,
    actual_days,
    single_strat,
    agg
):
    actual_days = format_datetime_list(actual_days)
    requested_days = format_datetime_list(requested_days)
    info_str = f"for {symbol} highest profit {highest_profit} when buying at {highest_profit_b} and selling at {highest_profit_s} with {transactions} transactions and invested time {invested_time} in seconds and elapsed time {time.time() - start_time} to calculate and range buy {rbm} and range sell {rsm} with memory {memory} and number of requested days {len(requested_days)} and number of actual days {len(actual_days)} and max_buy {max_buy}, min_buy {min_buy}, max_sell {max_sell}, min_sell {min_sell}, aggregate seconds {agg}"
    if single_strat:
        info_str = "Single strat "+info_str
    log(info_str)

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
        "requested_days_nb": len(requested_days),
        "requested_days": requested_days,
        "actual_days_nb": len(actual_days),
        "actual_days": actual_days,
        "data_source": data_source,
        "memory": memory,
        "max_buy":max_buy,
        "min_buy":min_buy,
        "max_sell":max_sell,
        "min_sell":min_sell,
        "single_strat":str(single_strat),
        "agg":agg
    }
    log(info)

    with open(f"log.txt", "a") as file:
        file.write(f"{info_str}\n")
        
    for key, val in info.items():
        if isinstance(val, int) or isinstance(val, float):
            info[key] = Decimal(str(val))
    dyn_resource = boto3.resource("dynamodb")
    table = dyn_resource.Table("Results")
    
    table.put_item(Item=info)

def format_datetime_list(days):
    if isinstance(days, int):
        return []
    new_list = []
    for day in days:
        if isinstance(day, datetime):
            new_list.append(day.strftime("%Y-%m-%d"))
        else:
            return days
    return new_list
        


def get_memory_usage():
    log(f"RAM Used (GB): {psutil.virtual_memory()[3] / 1000000000}")
    return psutil.virtual_memory()[3] / 1000000000


def analyse_results(results, symbol, start_time, requested_days, data_source, memory, actual_days, single_strat, agg):
    compiled = {}
    highest_profit = -100000
    highest_profit_b = highest_profit_s = transactions = invested_time = rbm = rsm = 0

    for result in results:
        buy, sell, rb, rs, profit, transactionsnb, itime = result.ravel()
        compiled.update({f"{buy}_{sell}_{rb}_{rs}": profit})

        if profit > highest_profit:
            highest_profit = profit
            highest_profit_b = buy
            highest_profit_s = sell
            transactions = transactionsnb
            invested_time = itime
            rbm = rb
            rsm = rs
    
    max_buy=-100
    min_buy=100
    max_sell=-100
    min_sell=100
    for k in compiled.keys():
        if highest_profit == compiled[k]:
            
            b = float(k.split('_')[0])
            s = float(k.split('_')[1])
            br = float(k.split('_')[2])
            sr = float(k.split('_')[3])
            if min_buy > (b-br):
                min_buy = b-br
            if min_sell > (s-sr):
                min_sell = s-sr
            if max_buy < (b+br):
                max_buy = b+br
            if max_sell < (s+sr):
                max_sell = s+sr
    

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
        requested_days,
        data_source,
        max_buy,
        min_buy,
        max_sell,
        min_sell,
        actual_days,
        single_strat,
        agg
    )
    return(highest_profit, max_buy, min_buy, max_sell, min_sell)
    # gen_csv(compiled)
    # show_graph(compiled)


def run_strats_gpu(strategies, data_cache):
    log("running on gpu")
    start_time = time.time()
    data = data_cache.data
    threadsperblock = 1024
    blockspergrid = math.ceil(strategies.shape[0] / threadsperblock)

    np_data = np.array(data)
    run_strat[blockspergrid, threadsperblock](strategies, np_data)
    return strategies, start_time


def run_strats_multiprocess(strategies):
    log("running on cpu")
    start_time = time.time()
    p = mp.Pool()
    results = p.starmap(run_single, tqdm.tqdm(strategies, total=len(strategies)))
    p.close()
    return results, start_time


def run_single(
    cache,
    buy_on_diff_percent,
    sell_on_diff_percent,
    range_b,
    range_s,
    agg
):
    data = cache.data
    strat_runner = Runner(data, 100000)
    profit, transactions, total_time_invested = strat_runner.run_strat(
        buy_on_diff_percent - (range_b),
        buy_on_diff_percent + (range_b),
        sell_on_diff_percent - (range_s),
        sell_on_diff_percent + (range_s),
        agg
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

def log(string):
    if not this.siltent_log:
        print(string)

if __name__ == "__main__":
    main()
