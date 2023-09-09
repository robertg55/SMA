import time
#from multiprocessing import Pool
#from torch.multiprocessing import Pool
import multiprocessing as mp
from data_fetcher import query_data
from runner import Runner
from datetime import datetime, timedelta
from graph_generator import gen_csv, show_graph
from functools import lru_cache
import tqdm
from numba import jit, cuda
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", default="yfinance", help="Data source", choices={"yfinance", "aws"})
    parser.add_argument("days", default=30, type=int, help="Number of days to compute")
    parser.add_argument("start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("end", default=16, type=float, help="End of day time")


class DataFetcher():
    
    def __init__(self):
        self.data = None
    
    def poll_yfinance(self, symbol, days_to_poll):
        print("YFINANCE FETCH")
        old_date = datetime.now() - timedelta(days=days_to_poll)
        month = [old_date + timedelta(idx + 1)for idx in range((datetime.now() - old_date).days)]
        data = []
        for day in month:
            data.extend(self.poll_yfinnance_day(symbol, start_time=day.strftime("%Y-%m-%d"), end_time=(day+ timedelta(1)).strftime("%Y-%m-%d")))
        self.data = data
    
    def poll_yfinnance_day(self, symbol, start_time=None, end_time=None):
        import yfinance as yf
        tickers = yf.Tickers(symbol)
        df = tickers.tickers[symbol].history(period="1d", interval="1m", start=start_time, end=end_time)
        data = []
        for index, row in df.iterrows():
            data.append((index.to_pydatetime().timestamp(), row["Open"]))
        return data
    
    def poll_aws_data(self, symbol, start_time=9.5, end_time=16):
        df = query_data(symbol, start_time, end_time)
        data = []
        for _, row in df.iterrows():
            data.append((row["time"], row["price"]))
        self.data = data
'''
def run_strats_multiprocess(poll_from_yfinance=False, days_to_poll=0):
    symbol = "TQQQ"
    #date = "2023-9-6"
    
    compiled = {}
    
    print("pulling data")
    if poll_from_yfinance:
        if days_to_poll:
             
            data = yfinance_multidays(symbol, days_to_poll)
            if len(data) != 0:
                run_strats_with_data(data, symbol, compiled)
            
                

        else:
            data = poll_yfinnance(symbol)
            run_strats_with_data(data, symbol, compiled)
            
    else:
        data = poll_aws_data(symbol)
        run_strats_with_data(data, symbol, compiled)
    
    max = 0
    kk=""
    for k in compiled.keys():
        if compiled[k] > max:
            max = compiled[k]
            kk = k
    print(f"max {max} buy {kk.split('_')[0]} sell {kk.split('_')[1]}")
    gen_csv(compiled)
    show_graph(compiled)
    

def run_strats_with_data(data, symbol, compiled): 

    start = time.time()
    highest_profit = -100000
    highest_profit_b = 0
    highest_profit_s = 0
    transactions = 0
    invested_time = 0  
    datapoints = [(data, b/10, -(s/10)) for b in range(-100, 100) for s in range(-100, 100)]
    p = mp.Pool(processes = 5)
    results = p.starmap(run_single, datapoints)
    p.close()
    for result in results:
        #print(result)
        buy, sell, profit, transactionsnb, itime = result.split("_")
        profit=float(profit)
        val = compiled.setdefault(f"{buy}_{sell}", 0)
        cumul = compiled.get(f"{buy}_{sell}",0) + profit
        compiled.update({f"{buy}_{sell}":cumul})
        
        
        if profit > highest_profit:
            highest_profit = profit
            highest_profit_b = buy
            highest_profit_s = sell
            transactions = transactionsnb
            invested_time = itime
    result_str = f'for {symbol} highest profit {highest_profit} at buy {highest_profit_b} and sell {highest_profit_s} with {transactions} transactions and {invested_time} invested time and data len {len(data)} and elapsed time {time.time() - start}'
    print(result_str)
    with open(f'log.txt', 'a') as file:
        file.write(f"{result_str}\n")
'''
    

def run_strats_no_data(symbol, compiled): 
    print("START")
    start = time.time()
    highest_profit = -100000
    highest_profit_b = 0
    highest_profit_s = 0
    transactions = 0
    invested_time = 0  
    rbm = 0
    rsm = 0
    cache = DataFetcher()
    cache.poll_yfinance("TQQQ", 2)
    datapoints = [(cache, b/10, -(s/10), br/10, sr/10) for b in range(-100, 100) for s in range(-100, 100) for br in range(0, 1) for sr in range(0, 1)]
    print(len(datapoints))
    p = mp.Pool(processes=5)
    results = p.starmap(run_single, tqdm.tqdm(datapoints, total=len(datapoints)))
    p.close()
    for result in results:
        #print(result)
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
    result_str = f'for {symbol} highest profit {highest_profit} at buy {highest_profit_b} and sell {highest_profit_s} with {transactions} transactions and {invested_time} invested time and elapsed time {time.time() - start} and range buy {rbm} and range sell {rsm}'
    print(result_str)
    with open(f'log.txt', 'a') as file:
        file.write(f"{result_str}\n")


def run_single(cache, buy_on_diff_percent, sell_on_diff_percent, range_b, range_s, symbol="TQQQ", day_range=2):
    data = cache.data
    strat_runner = Runner(data, 100000)
    profit, transactions, total_time_invested = strat_runner.run_strat(buy_on_diff_percent-(range_b/2), buy_on_diff_percent+(range_b/2), sell_on_diff_percent-(range_s/2), sell_on_diff_percent+(range_s/2))
    return f"{buy_on_diff_percent}_{sell_on_diff_percent}_{profit}_{transactions}_{total_time_invested}_{range_b}_{range_s}"  


if __name__ == "__main__":
    main()
    run_strats_no_data("TQQQ", {})
