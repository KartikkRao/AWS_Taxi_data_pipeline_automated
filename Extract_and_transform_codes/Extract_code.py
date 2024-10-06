import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import requests
from pyspark.sql.functions import *
from datetime import datetime 
import boto3
import io
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = 'taxi-data203'
date = '2024'
month = ['01','02','03','04','05','06']  #will automate this in the future wherein using datetime module extract the month and then run the code for that month 
s3 = boto3.client('s3') #no need of access key since iam role already has access to put data
for m in month:
    url1 = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{m}.parquet'
    url2 = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{m}.parquet'
    res1 = requests.get(url1)
    res2 = requests.get(url2)
    file_path1 = f'raw_data/yellow_taxi/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.parquet'
    file_path2 = f'raw_data/green_taxi/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.parquet'

    if res1.status_code == 200:
        s3.put_object(
            Bucket = bucket,
            Key = file_path1,
            Body=res1.content,
            Metadata={'source': 'cloudfront', 'file_type': 'parquet'},
            ContentType='application/x-parquet'
        )
    else:
        print(f"Failed to download {url1}: {res1.status_code}")

    if res2.status_code == 200:
        s3.put_object(
            Bucket = bucket,
            Key = file_path2,
            Body=res2.content,
            Metadata={'source': 'cloudfront', 'file_type': 'parquet'},
            ContentType='application/x-parquet'
        )
    else:
        print(f"Failed to download {url1}: {res2.status_code}")

job.commit()
