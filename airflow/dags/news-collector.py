import subprocess
import os
from os import path
from datetime import timedelta
from datetime import datetime
from minio import Minio


import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


def get_minio_client():
    minio_client = Minio(
        endpoint="minio:9000",
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False
    )
    return minio_client

def object_exists(minio_client,bucket,path):
    try:
        minio_client.stat_object(bucket, path)
        return True
    except Exception as err:         
        pass      
    return False

def set_bucket(bucket_name,minio_client):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

def set_filesystem(directory_fs):
    if not path.exists(directory_fs):
        os.makedirs(directory_fs)

num_noticias = Variable.get("num_noticias")
bucket = "news"
search_topic = Variable.get("search_topic")
directory_topic = search_topic.strip().replace(" ","_")
directory_fs = bucket+"/"+directory_topic+"/"
init_year = Variable.get("init_year")
init_month = Variable.get("init_month")
init_day = Variable.get("init_day")
minio_client = get_minio_client()
news_data_dir = os.environ["NEWS_DATA"]

def set_directories(ds, **kwargs):
    set_bucket("news",minio_client)
    set_filesystem(directory_fs)

def get_news(ds, **kwargs):
    execution_date = kwargs['execution_date']
    execution_year = str(execution_date.year)
    execution_month = str(execution_date.month).zfill(2)
    execution_day = str(execution_date.day).zfill(2)

    date = execution_year+"-"+execution_month+"-"+execution_day #"2020-01-01" #YY-MM-DD
    filename_s3 = directory_topic+"/"+date+".json"
    filename_fs = news_data_dir+"/"+bucket+"/"+directory_topic+"/"+date+".json"

    if not path.exists(filename_fs):
        subprocess.run(['sh', news_data_dir+'/libs/get_news.sh',num_noticias, search_topic, directory_topic,execution_year,execution_month,execution_day,bucket])
    minio_client.fput_object(bucket, filename_s3, filename_fs, content_type='application/json')

default_args = {
    'owner': 'micro-data-lake',
    'depends_on_past': False,    
    'start_date': datetime(int(init_year), int(init_month), int(init_day)),
    'end_date': datetime.now(),
    'email': ['abxda@micro-data-lake.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'News_Collector',
    default_args=default_args,
    description='Recolector de Noticias',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='Set_Directories',
    provide_context=True,
    python_callable=set_directories,
    dag=dag)

t2 = PythonOperator(
    task_id='Get_News',
    provide_context=True,
    python_callable=get_news,
    dag=dag)

t3 = BashOperator(
    task_id='Concluido',
    bash_command='printf "Finalizado {{ execution_date.strftime("%d-%m-%Y") }}"',
    dag=dag,
)

t1 >> t2 >> t3