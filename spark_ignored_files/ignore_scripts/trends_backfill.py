# Configuration related to Cassandra connector & Cluster
# This script aggregates metadata: follower count by language, game, genre and console
# Calculates trends over ALL day rows
import findspark
findspark.init()
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import when as sql_when
from pyspark.sql.functions import isnull as sql_isnull
from pyspark.sql.functions import lag as sql_lag
from pyspark.sql.window import Window


def create_sql_context(spark_job_name, spark_master='spark://ip-10-0-0-22.ec2.internal:7077'):
    sc = SparkContext(spark_master, spark_job_name)
    return SQLContext(sc)


def initialize_environment(cassandra_cluster='10.0.0.5,10.0.0.7,10.0.0.12,10.0.0.19'):

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 ' \
        '--conf spark.cassandra.connection.host=' + cassandra_cluster + ' pyspark-shell'


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(spark_sql_context, table_name, keys_space_name='insight'):
    return spark_sql_context.read \
                      .format('org.apache.spark.sql.cassandra') \
                      .options(table=table_name, keyspace=keys_space_name) \
                      .load()


def write_df_to_table(table_df, table_name, key_space_name='insight'):
    table_df.write \
            .format('org.apache.spark.sql.cassandra') \
            .mode('append') \
            .options(table=table_name, keyspace=key_space_name) \
            .save()


def spark_aggregator(table_df, initial_value, value_to_write='trend', partition_key='streamer',
                     clustering_key='timestamp'):

    table_df = table_df.withColumn('prev_value', sql_lag(table_df[initial_value]).over(window))
    return table_df.withColumn(value_to_write, sql_when((sql_isnull(table_df[initial_value] - table_df['prev_value']) |
                                                 (table_df['prev_value'] == 0)), 0)
                               .otherwise(table_df['follower_count'] - table_df['prev_value']))\
        .select(partition_key, clustering_key, 'trend')


job_name = 'trend_aggregator'
tables_to_read_and_write = [['twitch_day', 'twitch_trend'],
                            ['twitter_day', 'twitter_trend'],
                            ['youtube_day', 'youtube_trend']]

value_to_read = 'follower_count'
value_to_read_alias = 'follower_count'
# value_to_write = 'trend'

table_partition_key = 'streamer'
table_clustering_key = 'timestamp'

initialize_environment()
sql_context = create_sql_context(job_name)
window = Window.partitionBy('streamer').orderBy('timestamp')

for table_to_read, table_to_write in tables_to_read_and_write:
    original_table = load_and_get_table_df(sql_context, table_to_read)
    df = spark_aggregator(original_table, value_to_read)
    write_df_to_table(df, table_to_write)
