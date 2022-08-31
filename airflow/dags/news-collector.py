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