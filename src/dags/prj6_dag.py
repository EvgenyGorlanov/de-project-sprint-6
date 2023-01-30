from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum
import vertica_python

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': 'nutslyc_yandex_ru',       
             'password': '38pBHfBBIjxA',
             'database': 'dwh',
            'autocommit': True
}

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket = bucket,
        Key = key,
        Filename='/data/' + key
    ) 

def load_data_to_vertica_stg():
    with vertica_python.connect(**conn_info) as conn:
        curs = conn.cursor()
        insert_sql = "COPY NUTSLYC_YANDEX_RU__STAGING.group_log(group_id,user_id,user_id_from,event,event_dt) FROM LOCAL '/data/group_log.csv' DELIMITER ','"
        curs.execute(insert_sql)


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_prj_dag_get_data():
    task1 = PythonOperator(
        task_id=f'fetch_group_log.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )

    task2 = PythonOperator(
        task_id=f'stg_group_log',
        python_callable=load_data_to_vertica_stg,
        op_kwargs={},
    )
    
    task1 >> task2

dag_get_data = sprint6_prj_dag_get_data() 