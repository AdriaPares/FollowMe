from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'insight',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(minutes=20),
    # datetime.now() - timedelta(minutes=20) # could be useful for the future
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('spark_minute_to_hour', default_args=default_args, schedule_interval=timedelta(hours=1))

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
