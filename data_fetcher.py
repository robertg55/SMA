import boto3
import pandas as pd
from datetime import datetime, timedelta
import pickle
import os

class DataFetcher:
    def __init__(self, data=None, actual_days=None, requested_days=None):
        self.data = data
        self.actual_days = actual_days
        self.requested_days = requested_days

    def get_skip_today_value(self, include_partial_today, start_hour, end_hour):
        current_hour = datetime.now().hour
        skip_today = (
            True
            if not include_partial_today
            and current_hour >= start_hour
            and current_hour < end_hour
            else False
        )
        return skip_today

    def poll_data(
        self, source, symbol, start_hour, end_hour, days, include_partial_today, overwrite=False, tdelta=0
    ):
        if self.data is not None and not overwrite:
            return
        skip_today = self.get_skip_today_value(include_partial_today, start_hour, end_hour)
        file_name = self.get_file_name(source, symbol, start_hour, end_hour, days, skip_today, tdelta)
        if os.path.isfile(file_name):
            print("using cached data")
            with open(file_name, 'rb') as read_file:
                self.data = pickle.load(read_file)
        else:
            if source == "yfinance":
                self.poll_yfinance(symbol, days, skip_today, tdelta)
            else:
                self.poll_aws_data(symbol, start_hour, end_hour, days, skip_today, tdelta)
                
            with open(file_name, 'wb') as write_file:
                pickle.dump(self.data, write_file)

    def poll_yfinance(self, symbol, days_to_poll, skip_today=False, tdelta=0):
        print("Fetching yfinance")
        days = self.get_days_to_compute(days_to_poll, skip_today, tdelta)
        actual_days = []
        data = []
        for day in days:
            day_data = self.poll_yfinnance_day(symbol,start_time=day.strftime("%Y-%m-%d"),end_time=(day + timedelta(1)).strftime("%Y-%m-%d"))
            if day_data:
                data.extend(day_data)
                actual_days.append(day.strftime("%Y-%m-%d"))
        self.data = data
        self.actual_days = actual_days

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
        self, symbol, start_time=9.5, end_time=16, days_to_poll=1, skip_today=False, tdelta=0
    ):
        print("Fetching aws")
        days = self.get_days_to_compute(days_to_poll, skip_today, tdelta)
        df = self.query_data(symbol, start_time, end_time, days)
        data = []
        for _, row in df.iterrows():
            data.append((row["time"], row["price"]))
        self.data = data
        self.actual_days = self.compute_actual_days(df)

    def query_data(self, table, start_time=8, end_time=17, days=[]):
        dyn_resource = boto3.resource("dynamodb")
        table = dyn_resource.Table(table)
        
        response = table.scan()
        items = response['Items']
        page=0
        while 'LastEvaluatedKey' in response:
            page = page + 1
            print(f"polling dynamedb page {page}")
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        df = pd.DataFrame(items)
        df = df.astype(float)
        df = self.filter_open_only(df, start_time, end_time)
        df = self.filter_specific_days(df, days)
        df = df.rename(columns={"writetime": "time"})
        df = df.sort_values("time")
        return df

    def compute_actual_days(self, df):
        df["datetime-str"] = df["datetime"].dt.strftime("%Y-%m-%d")
        return df["datetime-str"].unique().tolist()


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

    def get_days_to_compute(self, days_to_poll, skip_today, tdelta):
        now = datetime.now() - timedelta(days=tdelta)
        old_date = now - timedelta(days=days_to_poll)
        month = [old_date + timedelta(idx + 1) for idx in range(days_to_poll)]
        self.requested_days = [day for day in month if not skip_today or day.strftime("%Y-%m-%d") != datetime.now().strftime("%Y-%m-%d")]
        return self.requested_days

    def get_file_name(self, source, symbol, start_hour, end_hour, days, skip_today, tdelta):
        
        days_arr = self.get_days_to_compute(days, skip_today, tdelta)
        start=str(days_arr[0].date())
        end=str(days_arr[-1].date())

        return os.path.join("cache", f"{source}-{symbol}-{start}-{end}-{start_hour}-{end_hour}")
