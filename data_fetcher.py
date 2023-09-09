import boto3
import pandas as pd
from datetime import datetime, timedelta

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
        df = self.query_data(symbol, start_time, end_time)
        data = []
        for _, row in df.iterrows():
            data.append((row["time"], row["price"]))
        self.data = data
    
    
    def query_data(self, table, start_time=8, end_time=17):
        dyn_resource = boto3.resource("dynamodb")
        table = dyn_resource.Table(table)
        response = table.scan()
        items = response['Items']
        df = pd.DataFrame(items)
        df = df.astype(float)
        df = self.filter_open_only(df, start_time, end_time)
        df = df.rename(columns={'writetime': 'time'})
        df = df.sort_values('time')
        return df 
        
    
    def filter_open_only(self, df, start_hour=8, end_hour=16):
        """
        Filter data by when the stock exchange is open only
        """
        df['datetime'] = pd.to_datetime(df['writetime'], unit='s', utc=True).map(lambda x: x.tz_convert('America/New_York'))
        df['hour'] = df.apply(lambda row: row['datetime'].hour + row['datetime'].minute/60, axis=1)
        df = df.drop(df[df['hour'] >= end_hour].index)
        df = df.drop(df[df['hour'] < start_hour].index)
        return df
