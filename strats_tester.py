import argparse
from data_fetcher import DataFetcher
from strats import run

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="TQQQ", help="Symbol")
    parser.add_argument(
        "--src", default="aws", help="Data source", choices={"yfinance", "aws"}
    )
    parser.add_argument(
        "--days", default=11, type=int, help="Number of days to compute"
    )
    parser.add_argument("--start", default=9.5, type=float, help="Start of day time")
    parser.add_argument("--end", default=16, type=float, help="End of day time")
    parser.add_argument("--buy", default=-3.0, type=float, help="Buy max percent to test")
    parser.add_argument("--sell", default=-0.5, type=float, help="Sell max percent to test")
    parser.add_argument(
        "--brange", default=0.7, type=float, help="Buy max percent range to test"
    )
    parser.add_argument(
        "--srange", default=4.9, type=float, help="Sell max percent range to test"
    )
    parser.add_argument(
        "--include-partial-today", action="store_true", help="By default it skip todays info"
    )
    parser.add_argument(
        "--tdelta", default=0, type=int, help="Number of days in the past from which to start pulling data"
    )
    args = parser.parse_args()
    data_cache = DataFetcher()
    run(data_cache, args.src, args.symbol, args.start, args.end, args.days, args.include_partial_today, True, args.buy, args.sell, args.brange, args.srange, True, args.tdelta)
    
if __name__ == "__main__":
    main()