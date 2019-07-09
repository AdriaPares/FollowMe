from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'insight',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 1),
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Run once at midnight every day
dag = DAG('spark_aggregations', default_args=default_args, schedule_interval='0 0 * * *', catchup=False)

user = 'ubuntu'
host = 'ec2-3-218-220-243.compute-1.amazonaws.com'
bash_script_aggregations = "'bash /home/ubuntu/cassandra_jobs/metadata_aggregations.sh'"

spark_aggregations = BashOperator(
    task_id='spark_aggregations',
    bash_command='ssh ' + user + '@' + host + ' ' + bash_script_aggregations,
    dag=dag)

