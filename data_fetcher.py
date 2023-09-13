import boto3
import pandas as pd
from datetime import datetime, timedelta


class DataFetcher:
    def __init__(self):
        self.data = None

    def poll_data(
        self, source, symbol, start_hour, end_hour, days, include_partial_today
    ):
        current_hour = datetime.now().hour
        skip_today = (
            True
            if not include_partial_today
            and current_hour >= start_hour
            and current_hour < end_hour
            else False
        )
        if source == "yfinance":
            return self.poll_yfinance(symbol, days, skip_today)
        else:
            return self.poll_aws_data(symbol, start_hour, end_hour, days, skip_today)

    def poll_yfinance(self, symbol, days_to_poll, skip_today=False):
        print("Fetching yfinance")
        days = self.get_days_to_compute(days_to_poll, skip_today)
        data = []
        for day in days:
            data.extend(
                self.poll_yfinnance_day(
                    symbol,
                    start_time=day.strftime("%Y-%m-%d"),
                    end_time=(day + timedelta(1)).strftime("%Y-%m-%d"),
                )
            )
        self.data = data
        return days

    def poll_yfinnance_day(self, symbol, start_time=None, end_time=None):
        import yfinance as yf

        tickers = yf.Tickers(symbol)
        df = tickers.tickers[symbol].history(
            period="1d", interval="1m", start=start_time, end=end_time
        )
        data = []
        for index, row in df.iterrows():
            data.append((index.to_pydatetime().timestamp(), round(row["Open"], 4)))
        return data

    def poll_aws_data(
        self, symbol, start_time=9.5, end_time=16, days_to_poll=1, skip_today=False
    ):
        print("Fetching aws")
        days = self.get_days_to_compute(days_to_poll, skip_today)
        df = self.query_data(symbol, start_time, end_time, days)
        data = []
        for _, row in df.iterrows():
            data.append((row["time"], row["price"]))
        self.data = data
        return days

    def query_data(self, table, start_time=8, end_time=17, days=[]):
        dyn_resource = boto3.resource("dynamodb")
        table = dyn_resource.Table(table)
        response = table.scan()
        items = response["Items"]
        df = pd.DataFrame(items)
        df = df.astype(float)
        df = self.filter_open_only(df, start_time, end_time)
        df = self.filter_specific_days(df, days)
        df = df.rename(columns={"writetime": "time"})
        df = df.sort_values("time")
        return df

    def filter_specific_days(self, df, days):
        df["datetime-str"] = df["datetime"].dt.strftime("%Y-%m-%d")
        df = df[df["datetime-str"].isin([d.strftime("%Y-%m-%d") for d in days])]
        return df

    def filter_open_only(self, df, start_hour=8, end_hour=16):
        """
        Filter data by when the stock exchange is open only
        """
        df["datetime"] = pd.to_datetime(df["writetime"], unit="s", utc=True).map(
            lambda x: x.tz_convert("America/New_York")
        )
        df["hour"] = df.apply(
            lambda row: row["datetime"].hour + row["datetime"].minute / 60, axis=1
        )
        df = df.drop(df[df["hour"] >= end_hour].index)
        df = df.drop(df[df["hour"] < start_hour].index)
        return df

    def get_days_to_compute(self, days_to_poll, skip_today):
        old_date = datetime.now() - timedelta(days=days_to_poll)
        month = [old_date + timedelta(idx + 1) for idx in range(days_to_poll)]
        return [
            day
            for day in month
            if not skip_today
            or day.strftime("%Y-%m-%d") != datetime.now().strftime("%Y-%m-%d")
        ]
