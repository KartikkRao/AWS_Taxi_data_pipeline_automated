import json
import boto3
import time  


redshift_data = boto3.client('redshift-data')
sqs_client = boto3.client('sqs')
def lambda_handler(event, context):
    errors = []
    
    for sqs_record in event['Records']:
        message = sqs_record['body']
        receipt_handle = sqs_record['receiptHandle']
        s3_event = json.loads(message)
        
        for record in s3_event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            if 'transformed_data/yellow_taxi/' in key or 'transformed_data/green_taxi/' in key:
                s3_path = f's3://{bucket}/{key}'
                
                sql = f"""
                COPY "taxi-model".taxi_trip_data
                FROM '{s3_path}'
                IAM_ROLE 'IAM_ROLE'
                CSV 
                IGNOREHEADER 1
                DELIMITER ',';
                """
                
                try:
                    response = redshift_data.execute_statement(
                        Database='taxi-data',
                        Sql=sql,
                        WorkgroupName='data-engineering-workgroup'  
                    )
                    
                    query_id = response['Id']
                    
                    while True:
                        status_response = redshift_data.describe_statement(Id=query_id)
                        status = status_response['Status']
                        
                        if status in ['ABORTED', 'FAILED']:
                            errors.append(f"Failed to process {s3_path}: {status_response.get('Error', 'Unknown error')}")
                            break
                        
                        if status == 'FINISHED':
                            break
                        
                        time.sleep(5) 
                        
                        sqs_client.delete_message(
                        QueueUrl='queue_url',  
                        ReceiptHandle=receipt_handle
                    )
                        
                except Exception as e:
                    errors.append(f"Failed to process {s3_path}: {e}")
                    
            else:
                continue  
            
    if errors:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Some records failed to ingest',
                'errors': errors
            })
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('All records processed successfully.')
        }
