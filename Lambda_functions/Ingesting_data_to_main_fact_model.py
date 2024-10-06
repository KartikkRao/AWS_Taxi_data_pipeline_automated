import json
import boto3
import time


redshift_data = boto3.client('redshift-data')
s3 = boto3.client('s3')

REDSHIFT_WORKGROUP_NAME = 'data-engineering-workgroup'  
REDSHIFT_DATABASE = 'taxi-data'
REDSHIFT_IAM_ROLE = 'IAM_ROLE'
BUCKET_NAME = 'taxi-data203'
SQL_FILE_KEY = 'sql_scripts/taxi_main_fact_loading.sql'  

def lambda_handler(event, context):
    try:
        
        sql_file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=SQL_FILE_KEY)
        sql_script = sql_file_obj['Body'].read().decode('utf-8')

       
        response = redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP_NAME,
            Database=REDSHIFT_DATABASE,
            Sql=sql_script,
            WithEvent=True
        )
        
        
        statement_id = response['Id']
        
        
        while True:
            status_response = redshift_data.describe_statement(Id=statement_id)
            status = status_response['Status']
            if status in ['FINISHED', 'FAILED','ABORTED']:
                break
            time.sleep(2)
        
        
        if status == 'FAILED':
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'SQL script execution failed',
                    'error': status_response.get('Error')
                })
            }

        
        return {
            'statusCode': 200,
            'body': json.dumps('SQL script executed successfully.')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error executing SQL script',
                'error': str(e)
            })
        }
