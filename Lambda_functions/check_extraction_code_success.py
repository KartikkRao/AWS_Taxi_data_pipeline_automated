import boto3
import json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'taxi-data203'
    folder = 'raw_data/yellow_taxi/'  
    folder1 = 'raw_data/green_taxi/'
        
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder
    )
        
    response1 = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder1
    )
    
    if 'Contents' in response:
        parquet_files = [obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/') and obj['Key'].endswith('.parquet')]
        
    if 'Contents' in response1:
        parquet_files1 = [obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/') and obj['Key'].endswith('.parquet')]
    
    
    if parquet_files and parquet_files1:
        return {
            'statusCode': 200,
            'body': json.dumps('Parquet files found'),
            'files_exist': True,
            'file_list': parquet_files
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('No Parquet files found'),
            'files_exist': False
        }
