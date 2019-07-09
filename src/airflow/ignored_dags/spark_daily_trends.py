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

dag = DAG('spark_aggregations', default_args=default_args, schedule_interval=timedelta(days=1))

user = 'ubuntu'
host = 'ec2-3-218-220-243.compute-1.amazonaws.com'
bash_script_aggregations = "'bash /home/ubuntu/cassandra_jobs/trends_update.sh'"

spark_aggregations = BashOperator(
    task_id='spark_aggregations',
    bash_command='ssh ' + user + '@' + host + ' ' + bash_script_aggregations,
    dag=dag)

