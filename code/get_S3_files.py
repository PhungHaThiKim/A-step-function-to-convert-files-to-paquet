import json
import urllib.parse
import boto3

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    my_bucket = s3.Bucket(event['bucket'])
    my_prefix = event['prefix']
    output_path = event['output_path']
    
    keys = []
    contents = []
    rs = []
    
    for object_summary in my_bucket.objects.filter(Prefix=my_prefix):
        if object_summary.key != my_prefix:
            response = s3_client.get_object(Bucket=event['bucket'], Key=object_summary.key)
            rs.append({'bucket':event['bucket'], 'output_path': output_path,
                        'key':object_summary.key, 'content': response['ContentType']})
       
    try:
        return rs
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e