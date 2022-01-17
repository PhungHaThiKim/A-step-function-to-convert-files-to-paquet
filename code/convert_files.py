import numpy 
import pandas 
import fastparquet
import boto3
import json
import urllib.parse

def lambda_handler(event, context):
    #identifying resource
    s3_resource = boto3.resource('s3')
    s3_object = boto3.client('s3', region_name='us-west-1')

    #access file
    bucket = event['bucket']
    key = urllib.parse.unquote_plus(event['key'], encoding='utf-8')
    output_path = event['output_path']
    
    get_file = s3_object.get_object(Bucket=bucket, Key=key)
    get = get_file['Body']
    
    
    if event['content'] == 'text/csv':
        url = output_path + key.split("/")[-1].split(".")[0]+"_csv_to_.parquet"
        df = pandas.DataFrame(get)
        
    elif event['content'] == 'application/json':
        url = output_path + key.split("/")[-1].split(".")[0]+"_json_to_.parquet"
        df = pandas.DataFrame.from_dict(get)
        
    elif event['content'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        url = output_path + key.split("/")[-1].split(".")[0]+"_xlsx_to_.parquet"
        df = pandas.DataFrame(get)

    else:
        return
    
    df.columns = df.columns.astype(str)
    df.to_parquet(url, engine='fastparquet')
    
    return 
