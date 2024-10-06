import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from datetime import datetime 
import boto3
import time 
from pyspark.sql.types import DateType,DoubleType,LongType,IntegerType
import json
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def default_changes(df):
    default_values = {
    "VendorID": 0,
    "passenger_count": 0,
    "trip_distance": 0.0,
    "pickup_date": '9999-12-31',
    "pickup_hours": 99,
    "pickup_minutes": 99,
    "pickup_seconds":99,
    "drop_date": '9999-12-31',
    "drop_hours": 99,
    "drop_minutes": 99,
    "drop_seconds":99,
    "RatecodeID": 0,
    "store_and_fwd_flag": 'N',
    "PULocationID": 0,
    "DOLocationID": 0,
    "payment_type": 0,
    "fare_amount": 0.0,
    "extra": 0.0,
    "mta_tax": 0.0,
    "tip_amount": 0.0,
    "tolls_amount": 0.0,
    "improvement_surcharge":0.0,
    "congestion_surcharge":0.0,
    "total_amount": 0.0,
    "trip_type": 0,
    "taxi_type": 'Unknown'
    }

    for column, default_value in default_values.items():
        df = df.fillna({column: default_value})

    df=df.withColumn('VendorID', when((col('VendorID') != 1) & (col('VendorID') != 2), 3).otherwise(col('VendorID')))
    df=df.withColumn('RatecodeID', when(col('RatecodeID')>6,7).otherwise(col('RatecodeID')))
    df=df.withColumn('payment_type', when(col('payment_type')>6,7).otherwise(col('payment_type')))
    df=df.withColumn('store_and_fwd_flag', when((col('store_and_fwd_flag') != 'Y') & (col('store_and_fwd_flag') != 'N'), 'N').otherwise(col('store_and_fwd_flag')))

    return df

def yellow_taxi_transform(df):
    df=df.withColumn('extra', col('extra')+col('Airport_fee'))
    df=df.drop( "Airport_fee" )
    df=df.withColumn('trip_type',lit(1))
    df=df.withColumn('taxi_type', lit('yellow'))
    df=df.withColumn('pickup_date' , to_date(col('tpep_pickup_datetime')))  # Did this to just learn the process otherwise this process will be handled in warehouse table forming
    df=df.withColumn('pickup_time' , date_format(to_timestamp(col('tpep_pickup_datetime')),"HH:mm:ss"))
    df=df.withColumn('drop_date' , to_date(col('tpep_dropoff_datetime')))
    df=df.withColumn('drop_time' , date_format(to_timestamp(col('tpep_dropoff_datetime')),"HH:mm:ss"))

    time = ['hours','minutes','seconds']
    for i in range(len(time)):
        df = df.withColumn(f'pickup_{time[i]}', split(col('pickup_time'), ':')[i].cast('int'))
        df = df.withColumn(f'drop_{time[i]}', split(col('drop_time'), ':')[i].cast('int'))

    df=df.drop('tpep_pickup_datetime','tpep_dropoff_datetime','pickup_time','drop_time')
    df=df.selectExpr('VendorID','passenger_count','trip_distance','pickup_date','pickup_hours','pickup_minutes','pickup_seconds','drop_date','drop_hours','drop_minutes','drop_seconds','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','congestion_surcharge','total_amount','trip_type','taxi_type')

    return df
    
def green_taxi_transform(df1):
    df1=df1.withColumn('extra',col('extra')+col('ehail_fee'))
    df1=df1.drop( "ehail_fee" )
    df1=df1.withColumn('taxi_type', lit('green'))
    df1=df1.withColumn('pickup_date' , to_date(col('lpep_pickup_datetime')))
    df1=df1.withColumn('pickup_time' , date_format(to_timestamp(col('lpep_pickup_datetime')),"HH:mm:ss"))
    df1=df1.withColumn('drop_date' , to_date(col('lpep_dropoff_datetime')))
    df1=df1.withColumn('drop_time' , date_format(to_timestamp(col('lpep_dropoff_datetime')),"HH:mm:ss"))

    time = ['hours','minutes','seconds']
    for i in range(len(time)):
        df1 = df1.withColumn(f'pickup_{time[i]}', split(col('pickup_time'), ':')[i].cast('int'))
        df1 = df1.withColumn(f'drop_{time[i]}', split(col('drop_time'), ':')[i].cast('int'))

    
    df1=df1.drop('lpep_pickup_datetime','lpep_dropoff_datetime','pickup_time','drop_time')
    df1=df1.selectExpr('VendorID','passenger_count','trip_distance','pickup_date','pickup_hours','pickup_minutes','pickup_seconds','drop_date','drop_hours','drop_minutes','drop_seconds','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','congestion_surcharge','total_amount','trip_type','taxi_type')

    return df1
    
s3_path1 = "s3://taxi-data203/raw_data/yellow_taxi/"
s3_path2 = "s3://taxi-data203/raw_data/green_taxi/"

try:
    df = spark.read.parquet(s3_path1)
except Exception as e:
    df = None
    
try:
    df1 = spark.read.parquet(s3_path2)
except Exception as e:
    df1 = None

if df and not df.isEmpty():
    df = yellow_taxi_transform(df)
    df = default_changes(df)
    output_path1 = f"s3://taxi-data203/transformed_data/yellow_taxi/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    df.write.mode("overwrite").option("header" , "true").format("csv").save(output_path1)
    
if df1 and not df1.isEmpty():
    df1 = green_taxi_transform(df1)
    df1 = default_changes(df1)
    output_path2 = f"s3://taxi-data203/transformed_data/green_taxi/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    df1.write.mode("overwrite").option("header" , "true").format("csv").save(output_path2)


s3 = boto3.client('s3')
bucket = 'taxi-data203'
key1 = 'raw_data/yellow_taxi/'
key2 = 'raw_data/green_taxi/'
file_key1 = []
file_key2 = []

if 'Contents' in s3.list_objects(Bucket=bucket, Prefix=key1):
    for file in s3.list_objects(Bucket = bucket, Prefix = key1)['Contents']:
        if file['Key'].split('.')[-1] == 'parquet':
            file_key1.append(file['Key'])

if 'Contents' in s3.list_objects(Bucket=bucket, Prefix=key2):    
    for file in s3.list_objects(Bucket = bucket, Prefix = key2)['Contents']:
        if file['Key'].split('.')[-1] == 'parquet':
            file_key2.append(file['Key'])

for key in file_key1:
    try:
        s3.delete_object(Bucket = bucket , Key = key)   # In future will be deleting the whole raw file and creating it in extraction code, current one is for practice of various command in another project this logic shall be implemented
    except Exception as e:
        print(f"Error deleting {key}: {e}")

for key in file_key2:
    try:
        s3.delete_object(Bucket = bucket , Key = key)
    except Exception as e:
        print(f"Error deleting {key}: {e}")
  
job.commit()
