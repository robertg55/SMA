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
        "--days-group", default=7, type=int, help="Number of days to group on simulation"
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
    args = parser.parse_args()
    
    data_cache = DataFetcher()
    run_simulation(data_cache, args.src, args.symbol, args.days_group, args.start, args.end, args.include_partial_today, args.buy, args.sell, args.brange, args.srange)
    
    
def run_simulation(data_cache, src, symbol, days_group, start, end, include_partial_today, buy, sell, brange, srange):
    data_cache.poll_data(
        src,
        symbol,
        start,
        end,
        100,
        include_partial_today,
    )
    single_days = group_by_day(data_cache)
    print(f"{single_days.keys()}")
    oldest_day = datetime.strptime(sorted(list(single_days.keys()))[0], '%Y-%m-%d')
    evaluating_day=oldest_day+timedelta(days=days_group)
    end_day = datetime.today()
    print("simulation start")
    strategies = get_strategies(buy, sell, brange, srange, False)
    while(evaluating_day.date()!=end_day.date()):
        if str(evaluating_day.date()) in list(single_days.keys()):
            grouped_list = []
            for day in range(days_group):
                grouped_list.extend(single_days.get(str(evaluating_day.date()-timedelta(days=(1+day))), []))
            if grouped_list:
                grouped_data_cache = DataFetcher(grouped_list, days_group, days_group)
                _, max_buy, min_buy, max_sell, min_sell = run(grouped_data_cache, src, symbol, start, end, days_group, False, False, buy, sell, brange, srange, False, 0, True, strategies)
                b = (max_buy + min_buy)/2
                s = (max_sell + min_sell)/2
                br = max_buy-min_buy
                sr = max_sell-min_sell
                single_data_cache = DataFetcher(single_days.get(str(evaluating_day.date())), 1, 1)
                eval_highest_profit, _, _, _, _ = run(single_data_cache, src, symbol, start, end, 1, include_partial_today, True, b, s, br, sr, True, 0, True)
                print(f"simulation {eval_highest_profit} with buy {b}, sell {s}, brange {br}, srange {sr}")
            else:
                print(f"grouped_list empty for {evaluating_day}")
        evaluating_day = evaluating_day+timedelta(days=1)
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