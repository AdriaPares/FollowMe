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
    'retry_delay': timedelta(minutes=1)
}

# Run once an hour at the beginning of the hour, except for hour zero (midnight)
# At that time, hour_to_day runs this dag as a subset of its execution, so we don't require this one
dag = DAG('spark_minute_to_hour', default_args=default_args, schedule_interval='0 1-23 * * *', catchup=False)

user = 'ubuntu'
host = 'ec2-3-218-220-243.compute-1.amazonaws.com'
bash_script_live_to_minute = "'bash /home/ubuntu/cassandra_jobs/spark_live_to_minute.sh'"
bash_script_minute_to_hour = "'bash /home/ubuntu/cassandra_jobs/spark_minute_to_hour.sh'"

spark_live_to_minute = BashOperator(
    task_id='spark_live_to_minute',
    bash_command='ssh ' + user + '@' + host + ' ' + bash_script_live_to_minute,
    dag=dag)

spark_minute_to_hour = BashOperator(
    task_id='spark_minute_to_hour',
    bash_command='ssh ' + user + '@' + host + ' ' + bash_script_minute_to_hour,
    dag=dag)

spark_live_to_minute >> spark_minute_to_hour
