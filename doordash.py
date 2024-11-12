import json
import boto3
from dotenv import load_dotenv
import os
import pandas as pd
from io import StringIO

file = '/home/advait/Documents/DE_category_id.json'
f = '/home/advait/PycharmProjects/dm/temp.json'
loc = '/home/advait/Documents/dataset.csv'

api_key = os.getenv('Access_key')
api_secret = os.getenv('Secret_access_key')
bucket_name = 'doordash-landing-zn-ad'


def list_bucket():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    obj = s3.get_object(Bucket='doordash-landing-zn-ad', Key='DE_category_id.json')
    body = obj['Body'].read().decode()
    js = json.loads(body)
    data = js['items']
    data_list = []
    for data in data:
        data_list.append({
            'id': data['id'],
            'snippet': data['snippet']['title'],
            'assignable': data['snippet']['assignable']
        })
    df = pd.DataFrame(data_list)
    print(df)
    # # df.to_json('temp.json')
    #
    # s3.upload_file(f, 'doordash-target-zn-ad', 'temp.json')
    # print("successfully upload")


# list_bucket()

def upload_file():
    s3 = boto3.client('s3')
    # df = pd.read_csv(loc,dtype='unicode')
    s3.upload_file(loc,'doordash-target-zn-ad', 'dataset.csv')

upload_file()