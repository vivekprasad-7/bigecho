from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import os


def print_hello():
    sqoopcom= "sqoop import --connect jdbc:mysql://localhost/firehose --username               root -password cloudera  --table abc --m 1 --target-dir /TabcDAG"
    os.system(sqoopcom)

abc = 'python /root/Desktop/POC/AllDAGS/scripts/sqoop_import.py'
vv = 'python /root/Desktop/POC/Validation/yaml_Validation.py'
sp = 'pyspark /root/Desktop/POC/Loyalty/demo.py'

dag = DAG('validDAG', description='Simple tutorial DAG',
                schedule_interval='*/15 * * * *',
                   start_date=datetime(2017, 3, 20), catchup=False)

#sqoop_operator = BashOperator(task_id='sqoop_task', bash_command=abc, dag=dag)
valid_operator = BashOperator(task_id='validity_task', bash_command=vv, dag=dag)
etl_operator = BashOperator(task_id='ETL_task', bash_command=sp, dag=dag)

#etl_operator
#valid_operator >> sqoop_operator
valid_operator >> etl_operator
#sqoop_operator
