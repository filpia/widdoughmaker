
from retry import retry
import logging
from io import BytesIO
import numpy as np
import pandas as pd
import s3urls
import boto3

logging.basicConfig()


def timestamp_to_unix_int64(t):
    """_summary_

    :param t: _description_
    :type t: _type_
    :return: _description_
    :rtype: _type_
    """
    return np.int64(t.timestamp())

def construct_s3_query_results_location(bucket='wid-athena-results', subdir='query_results'):
    """_summary_

    :param bucket: _description_, defaults to 'wid-athena-results'
    :type bucket: _type_, optional
    :param subdir: _description_, defaults to 'query_results'
    :type subdir: _type_, optional
    :return: _description_
    :rtype: _type_
    """
    query_id = f'query_{timestamp_to_unix_int64(pd.Timestamp.now())}.csv'

    return f's3://{bucket}/{subdir}/{query_id}'


@retry(AssertionError, delay=3, backoff=2, tries=10, max_delay=30)
def check_success(athena_client, query_execution_id):
    """_summary_
    Retry with backoff waiting 3, 6, 12, 24, 30, ... 30 sec up to 10 retries
    :param query_execution: _description_
    :type query_execution: _type_
    :return: _description_
    :rtype: _type_
    """
    query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    if query_execution['QueryExecution']['Status']['State'] == 'FAILURE':
        return {
            'status': query_execution['QueryExecution']['Status']['State'],
            'failure_reason': query_execution['QueryExecution']['Status']['StateChangeReason'],
            's3_path': None,
            'success': False
            
        }
    assert query_execution['QueryExecution']['Status']['State'] == 'SUCCEEDED'
    return {
            'status': query_execution['QueryExecution']['Status']['State'],
            's3_path': query_execution['QueryExecution']['ResultConfiguration']['OutputLocation'],
            'success': True
        }


def download_s3_csv_to_dataframe(s3_client, bucket, key):
    """_summary_

    :param s3_client: _description_
    :type s3_client: _type_
    :param bucket: _description_
    :type bucket: _type_
    :param key: _description_
    :type key: _type_
    :return: _description_
    :rtype: _type_
    """
    bio = BytesIO()
    s3_client.download_fileobj(
        Bucket=bucket,
        Key=key,
        Fileobj=bio
        )
    bio.seek(0)
    return pd.read_csv(bio)


def run_athena_query(q, athena_client=None, s3_client=None):
    """_summary_

    :param q: _description_
    :type q: _type_
    :param athena_client: _description_
    :type athena_client: _type_
    :param s3_client: _description_
    :type s3_client: _type_
    :return: _description_
    :rtype: _type_
    """

    if not athena_client:
        athena_client = boto3.client('athena')
    if not s3_client:
        s3_client = boto3.client('s3')
    # kick off query
    print('Starting Query Execution')
    start_query = athena_client.start_query_execution(
        QueryString = q,
        ResultConfiguration = {
            'OutputLocation': construct_s3_query_results_location()
        }
    )
    # check to see if query finished. Response will have s3
    print('Checking if query has completed')
    athena_job_response = check_success(athena_client=athena_client, query_execution_id=start_query['QueryExecutionId'])
    assert athena_job_response['success'], f'Athena query failed with error {athena_job_response["failure_reason"]}'
    # use helper to get bucket/key
    parsed_uri = s3urls.parse_url(athena_job_response['s3_path'])
    # download from s3 to in-memory df
    print('Downloading results into dataframe')
    df = download_s3_csv_to_dataframe(
        s3_client=s3_client,
        bucket=parsed_uri['bucket'],
        key=parsed_uri['key']
    )
    return df

    

