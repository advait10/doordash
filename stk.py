import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import json
import boto3


print("hiiiiiiiiiiiiiiii")
df = yf.download(tickers=['NHPC.NS'], period='1d', interval='1m')
df.reset_index(inplace=True)
df['Timestamp'] = pd.to_datetime(df['Datetime'])
df['Timestamp'] = df['Timestamp'] + pd.to_timedelta(df.groupby('Timestamp').cumcount(), unit='m')
df.to_csv('data')
f = '/home/advait/PycharmProjects/dm/data'
def upload_file():
    s3 = boto3.client('s3')
    s3.upload_file(f,'doordash-target-zn-ad', 'NHPC.csv')

upload_file()