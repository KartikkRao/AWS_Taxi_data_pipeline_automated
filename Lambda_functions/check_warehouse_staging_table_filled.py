import boto3
import json
import time

client = boto3.client('redshift-data')

def lambda_handler(event, context):
    
    work_group_name = 'data-engineering-workgroup'
    database_name = 'taxi-data'
    
    query = """SELECT COUNT(*) AS row_count FROM "taxi-model".taxi_trip_data;"""
        
        
    response = client.execute_statement(
        WorkgroupName=work_group_name,
        Database=database_name,
        Sql=query
    )
    
        
    query_id = response['Id']
        
    status = 'SUBMITTED'
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        result = client.describe_statement(Id=query_id)
        status = result['Status']
        print(f"Query status: {status}")
        if status not in ['FINISHED', 'FAILED', 'ABORTED']:
            time.sleep(2) 
    print(result)    
    if status == 'FINISHED':
            # If the query has a result set, fetch the results
        if result['HasResultSet']:
            results = client.get_statement_result(Id=query_id)
            print(f"Query Results: {results}")
                
            if 'Records' in results:
                row_count = results['Records'][0][0]['longValue']
                    
                if row_count > 0:
                    return {
                        'statusCode': 200,
                            'body': json.dumps(f'Table has {row_count} rows'),
                            'table_has_data': True
                        }
                else:
                    return {
                            'statusCode': 200,
                            'body': json.dumps('Table is empty'),
                            'table_has_data': False
                        }
            else:
                return{
                            'statusCode': 200,
                            'body': json.dumps('No records'),
                            'table_has_data': False
                    }
        else:
            return{
                        'statusCode': 200,
                        'body': json.dumps('No result produced'),
                        'table_has_data': False
                }
        
    elif status == 'FAILED':
        return{
                    'statusCode': 400,
                    'body': json.dumps('failed'),
                    'table_has_data': False
            }
                            
        
    elif status == 'ABORTED':
        
        return{
                    'statusCode': 500,
                    'body': json.dumps('abort'),
                    'table_has_data': False
            }
