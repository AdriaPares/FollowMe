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
    'retry_delay': timedelta(seconds=10),

}

# Runs every minute at the beginning of the minute, except when it's the first minute of the hour (14:00, for example)
# At that time, minute_to_hour runs this dag as a subset of its execution, so we don't require this one
# Same idea for noon, at that time we will have hour_to_day running, which contains minute_to_hour already
dag = DAG('spark_live_to_minute', default_args=default_args, schedule_interval='1-59 * * * *', catchup=False)

user = 'ubuntu'
host = 'ec2-3-218-220-243.compute-1.amazonaws.com'
bash_script_live_to_minute = "'bash /home/ubuntu/cassandra_jobs/spark_live_to_minute.sh'"

spark_live_to_minute = BashOperator(
    task_id='spark_live_to_minute',
    bash_command='ssh ' + user + '@' + host + ' ' + bash_script_live_to_minute,
    dag=dag)
