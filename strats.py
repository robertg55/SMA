import time
import multiprocessing as mp
from runner import Runner
from data_fetcher import DataFetcher
import tqdm
import argparse
from graph_generator import gen_csv, show_graph

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="TQQQ", help="Symbol")
    parser.add_argument("--src", default="yfinance", help="Data source", choices={"yfinance", "aws"})
    parser.add_argument("--days", default=2, type=int, help="Number of days to compute")
    parser.add_argument("--start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("--end", default=16, type=float, help="End of day time")
    parser.add_argument("--buy", default=10, type=int, help="Buy max percent to test")
    parser.add_argument("--sell", default=10, type=int, help="Sell max percent to test")
    parser.add_argument("--brange", default=1, type=int, help="Buy max percent range to test")
    parser.add_argument("--srange", default=1, type=int, help="Sell max percent range to test")
    args = parser.parse_args()

    data_cache = DataFetcher()
    if args.src == "yfinance":
        data_cache.poll_yfinance(args.symbol, args.days)
    else:
        data_cache.poll_aws_data(args.symbol, args.start, args.end)
    datapoints = get_datapoints(data_cache, args.buy, args.sell, args.brange, args.srange)
    results, start_time = run_strats_multiprocess(datapoints)
    analyse_results(results, args.symbol, start_time)

def get_datapoints(data_cache, buy, sell, brange, srange):
    
    datapoints = [(data_cache, b/10, -(s/10), br/10, sr/10) for b in range(buy*(-10), buy*10+1) for s in range(sell*(-10), sell*10+1) for br in range(0, brange*10+1) for sr in range(0, srange*10+1)]
    print(f"number of datapoints: {len(datapoints)}")
    return datapoints

def analyse_results(results, symbol, start_time):
    compiled = {}
    highest_profit = -100000
    highest_profit_b = 0
    highest_profit_s = 0
    transactions = 0
    invested_time = 0  
    rbm = 0
    rsm = 0
    
    for result in results:
        buy, sell, profit, transactionsnb, itime, rb, rs= result.split("_")
        profit=float(profit)
        val = compiled.setdefault(f"{buy}_{sell}", 0)
        cumul = val + profit
        compiled.update({f"{buy}_{sell}":cumul})

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
    print(f"max {max_val} buy {key_val.split('_')[0]} sell {key_val.split('_')[1]}")
    #gen_csv(compiled)
    #show_graph(compiled)

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