import boto3
import pandas as pd

def query_data(table, start_time=8, end_time=17):
    dyn_resource = boto3.resource("dynamodb")
    table = dyn_resource.Table(table)
    response = table.scan()
    items = response['Items']
    df = pd.DataFrame(items)
    df = df.astype(float)
    df = filter_open_only(df, start_time, end_time)
    df = df.rename(columns={'writetime': 'time'})
    df = df.sort_values('time')
    return df 
    

def filter_open_only(df, start_hour=8, end_hour=17):
    """
    Filter data by when the stock exchange is open only
    """
    df['datetime'] = pd.to_datetime(df['writetime'], unit='s', utc=True).map(lambda x: x.tz_convert('America/New_York'))
    df['hour'] = df.apply(lambda row: row['datetime'].hour + row['datetime'].minute/60, axis=1)
    df = df.drop(df[df['hour'] >= end_hour].index)
    df = df.drop(df[df['hour'] < start_hour].index)
    return df


if __name__ == "__main__":
    print(query_data("TQQQ"))
