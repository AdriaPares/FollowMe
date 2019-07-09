# Configuration related to Cassandra connector & Cluster
# This will probably be done by Pulsar
import findspark
findspark.init()
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import udf, StringType
from pyspark.sql.functions import mean as sql_mean


def create_sql_context(spark_job_name, spark_master='spark://ip-10-0-0-22.ec2.internal:7077'):
    sc = SparkContext(spark_master, spark_job_name)
    return SQLContext(sc)


def initialize_environment(cassandra_cluster='10.0.0.5,10.0.0.7,10.0.0.12,10.0.0.19'):

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 ' \
        '--conf spark.cassandra.connection.host=' + cassandra_cluster + ' pyspark-shell'


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(sql_context, table_name, keys_space_name='insight'):
    return sql_context.read \
                      .format('org.apache.spark.sql.cassandra') \
                      .options(table=table_name, keyspace=keys_space_name) \
                      .load()


def write_df_to_table(table_df, table_name, key_space_name='insight'):
    table_df.write \
            .format('org.apache.spark.sql.cassandra') \
            .mode('append') \
            .options(table=table_name, keyspace=key_space_name) \
            .save()


def second_aggregator(timestamp_second):
    return timestamp_second[:16]+timestamp_second[19:]


def minute_aggregator(timestamp_minute):
    return timestamp_minute[:13] + timestamp_minute[16:]


def hour_aggregator(timestamp_hour):
    return timestamp_hour[:10] + timestamp_hour[13:]


def spark_aggregator(udf_agg, table_df, initial_value, new_value_alias, partition_key='streamer',
                     clustering_key='timestamp', clustering_key_alias='timestamp'):

    return table_df.groupBy(partition_key, udf_agg(clustering_key).alias(clustering_key_alias)) \
                   .agg(sql_mean(initial_value).alias(new_value_alias))


job_name = 'twitch_hour_to_day'
table_to_read = 'twitch_hour'
table_to_write = 'twitch_day'
value_name = 'follower_count'
value_alias = 'follower_count'

initialize_environment()
new_sql_context = create_sql_context(job_name)
original_table = load_and_get_table_df(new_sql_context, table_to_read)
udf_aggregator = udf(hour_aggregator, StringType())
new_table = spark_aggregator(udf_aggregator, original_table, value_name, value_alias)
write_df_to_table(new_table, table_to_write)
