import argparse
from data_fetcher import DataFetcher
from strats import run, get_strategies
from datetime import datetime, timedelta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="TQQQ", help="Symbol")
    parser.add_argument(
        "--src", default="aws", help="Data source", choices={"yfinance", "aws"}
    )
    parser.add_argument(
        "--days-group", default=5, type=int, help="Number of days to group on simulation"
    )
    parser.add_argument("--start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("--end", default=16, type=float, help="End of day time")
    parser.add_argument(
        "--include-partial-today", action="store_true", help="By default it skip todays info"
    )
    parser.add_argument("--buy", default=10, type=float, help="Buy max percent to test")
    parser.add_argument("--sell", default=10, type=float, help="Sell max percent to test")
    parser.add_argument(
        "--brange", default=5, type=float, help="Buy max percent range to test"
    )
    parser.add_argument(
        "--srange", default=5, type=float, help="Sell max percent range to test"
    )
    parser.add_argument(
        "--agg", default=60, type=int, help="Number of seconds to aggregate"
    )
    args = parser.parse_args()
    
    data_cache = DataFetcher()
    run_simulation(data_cache, args.src, args.symbol, args.days_group, args.start, args.end, args.include_partial_today, args.buy, args.sell, args.brange, args.srange, args.agg)
    
    
def run_simulation(data_cache, src, symbol, days_group, start, end, include_partial_today, buy, sell, brange, srange, agg):
    data_cache.poll_data(
        src,
        symbol,
        start,
        end,
        100,
        include_partial_today,
    )
    single_days = group_by_day(data_cache)
    days_arr = sorted(list(single_days.keys()))
    print(f"target days: {days_arr}")
    index = days_group
    evaluating_day=days_arr[index]
    print("simulation start")
    while(index < len(days_arr)):
        evaluating_day = days_arr[index]
        if evaluating_day in list(single_days.keys()):
            strategies = get_strategies(buy, sell, brange, srange, False, agg)
            grouped_list = []
            for day in range(days_group):
                grouped_list.extend(single_days.get(days_arr[index-1-day]))

            grouped_data_cache = DataFetcher(grouped_list, days_group, days_group)
            _, max_buy, min_buy, max_sell, min_sell = run(grouped_data_cache, src, symbol, start, end, days_group, False, False, buy, sell, brange, srange, False, 0, True, strategies, agg)
            b = (max_buy + min_buy)/2
            s = (max_sell + min_sell)/2
            br = max_buy-min_buy
            sr = max_sell-min_sell
            single_data_cache = DataFetcher(single_days.get(evaluating_day), 1, 1)
            eval_highest_profit, _, _, _, _ = run(single_data_cache, src, symbol, start, end, 1, include_partial_today, True, b, s, br, sr, True, 0, True, None, agg)
            info_str = f"{datetime.now()}: simulation {eval_highest_profit} with buy {b}, sell {s}, brange {br}, srange {sr} date {evaluating_day}"
            print(info_str)
            with open(f"log.txt", "a") as file:
                file.write(f"{info_str}\n")
        
        index = index + 1
    print("simulation done")

def group_by_day(data_cache):
    dates_dict = {}
    for item in data_cache.data:
        date=str(datetime.fromtimestamp(item[0]).date())
        date_list = dates_dict.setdefault(date, [])
        date_list.append(item)
    return dates_dict
        

if __name__ == "__main__":
    main()