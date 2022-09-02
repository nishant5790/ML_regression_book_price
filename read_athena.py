import boto3
import s3fs
import dns.resolver
import time
import re
import os
import pandas as pd
import multiprocessing
from tqdm import tqdm
tqdm.pandas()
import warnings
warnings.filterwarnings("ignore")


#Read domain details form ATHENA
def get_params(query):
    params = {
        'region': 'REGION',
        'database': 'DB',
        'bucket': 'test-nis',
        'path': 'athena-query-results',
        'query': query
        }
    return params

def athena_query(client, params):
    
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_to_s3(session, params, max_execution = 5):
    client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(30)
    
    return False

def get_prod_bw_s3_path(dm_id):
    client = boto3.client('athena')
        
    query = "Select * from table "
    params = get_params(query)

    session = boto3.Session()

    # Query Athena and get the s3 filename as a result
    s3_filename = athena_to_s3(session, params)
    #cleanup(session, params)
    return os.path.join('s3://', params['bucket'], params['path'], s3_filename)
