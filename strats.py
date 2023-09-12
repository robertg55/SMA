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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="TQQQ", help="Symbol")
    parser.add_argument("--src", default="yfinance", help="Data source", choices={"yfinance", "aws"})
    parser.add_argument("--days", default=2, type=int, help="Number of days to compute")
    parser.add_argument("--start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("--end", default=16, type=float, help="End of day time")
    parser.add_argument("--buy", default=10, type=int, help="Buy max percent to test")
    parser.add_argument("--sell", default=10, type=int, help="Sell max percent to test")
    parser.add_argument("--brange", default=4, type=int, help="Buy max percent range to test")
    parser.add_argument("--srange", default=4, type=int, help="Sell max percent range to test")
    parser.add_argument("--cpu", action='store_true', help="Use CPU instead of gpu")
    args = parser.parse_args()

    data_cache = DataFetcher()
    if args.src == "yfinance":
        data_cache.poll_yfinance(args.symbol, args.days)
    else:
        data_cache.poll_aws_data(args.symbol, args.start, args.end)
    
    if args.cpu:
        datapoints = get_starmap_datapoints(data_cache, args.buy, args.sell, args.brange, args.srange)
    else:
        datapoints = get_datapoints(args.buy, args.sell, args.brange, args.srange)

    if args.cpu:
        results, start_time = run_strats_multiprocess(datapoints)
    else:
        results, start_time = run_strats_gpu(datapoints, data_cache)
    analyse_results(results, args.symbol, start_time)

def get_starmap_datapoints(data_cache, buy, sell, brange, srange):
    
    datapoints = [(data_cache, b/10, -(s/10), br/10, sr/10) for b in range(buy*(-10), buy*10+1) for s in range(sell*(-10), sell*10+1) for br in range(0, brange*10+1) for sr in range(0, srange*10+1)]
    print(f"number of datapoints: {len(datapoints)}")
    return datapoints

def get_datapoints(buy, sell, brange, srange):
    def get_index(buy, sell, brange, srange):
        return (20*buy+1)*(20*sell+1)*(brange*10+1)*(srange*10+1)
    array = np.empty(shape=(get_index(buy, sell, brange, srange), 7), dtype=float)
    i = 0
    for b in range(buy*(-10), buy*10+1):
        for s in range(sell*(-10), sell*10+1):
            for br in range(0, brange*10+1):
                for sr in range(0, srange*10+1):
                    
                    array[i]=np.array([b/10, -(s/10), br/10, sr/10, 0, 0, 0])
                    i = i + 1
    print(f"number of datapoints: {len(array)}")
    return array

def analyse_results(results, symbol, start_time, cpu=False):
    compiled = {}
    highest_profit = -100000
    highest_profit_b = highest_profit_b = transactions = invested_time = rbm = rsm = 0
    
    for result in results:
        if cpu:
            buy, sell, profit, transactionsnb, itime, rb, rs= result.split("_")
            profit=float(profit)

        else:
            buy, sell, rb, rs, profit, transactionsnb, itime = result.ravel()

        cumul = compiled.setdefault(f"{buy}_{sell}_{rb}_{rs}", 0) + profit
        compiled.update({f"{buy}_{sell}_{rb}_{rs}":cumul})

        if profit > highest_profit:
            highest_profit = profit
            highest_profit_b = buy
            highest_profit_s = sell
            transactions = transactionsnb
            invested_time = itime
            rbm = rb
            rsm = rs
    result_str = f'for {symbol} highest profit {highest_profit} when buying at {highest_profit_b} and selling at {highest_profit_s} with {transactions} transactions and invested time {invested_time} in seconds and elapsed time {time.time() - start_time} to calculate and range buy {rbm} and range sell {rsm}'
    print(result_str)
    with open(f'log.txt', 'a') as file:
        file.write(f"{result_str}\n")
    max_val = 0
    key_val=""
    for k in compiled.keys():
        if compiled[k] > max_val:
            max_val = compiled[k]
            key_val = k
    print(f"multiple day cumulative profit: max {max_val} for buy at {key_val.split('_')[0]} and sell at {key_val.split('_')[1]} with rangeB {key_val.split('_')[2]} and rangeS {key_val.split('_')[3]}")
    #gen_csv(compiled)
    #show_graph(compiled)

def run_strats_gpu(datapoints, data_cache): 
    start_time = time.time()
    data = data_cache.data
    threadsperblock = 1024
    blockspergrid = math.ceil(datapoints.shape[0] / threadsperblock)

    np_data = np.array(data)
    run_strat[blockspergrid, threadsperblock](datapoints, np_data)
    
    return datapoints, start_time
 
def run_strats_multiprocess(datapoints): 
    start_time = time.time()
    p = mp.Pool()
    results = p.starmap(run_single, tqdm.tqdm(datapoints, total=len(datapoints)))
    p.close()
    return results, start_time


def run_single(cache, buy_on_diff_percent, sell_on_diff_percent, range_b, range_s, symbol="TQQQ", day_range=2):
    data = cache.data
    strat_runner = Runner(data, 100000)
    profit, transactions, total_time_invested = strat_runner.run_strat(buy_on_diff_percent-(range_b/2), buy_on_diff_percent+(range_b/2), sell_on_diff_percent-(range_s/2), sell_on_diff_percent+(range_s/2))
    return f"{buy_on_diff_percent}_{sell_on_diff_percent}_{profit}_{transactions}_{total_time_invested}_{range_b}_{range_s}"  


if __name__ == "__main__":
    main()