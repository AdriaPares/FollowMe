import findspark
findspark.init()
import os
from pyspark.sql import SQLContext, dataframe, context
from pyspark import SparkContext
from pyspark.sql.functions import mean as sql_mean
from pyspark.sql.functions import udf, StringType, col


def create_sql_context(spark_job_name: str, spark_master: str = 'spark://ip-10-0-0-22.ec2.internal:7077') -> context:
    sc = SparkContext(spark_master, spark_job_name)
    return SQLContext(sc)


# Initializes Cassandra Cluster and Cassandra Driver
def initialize_environment(cassandra_cluster: str = '10.0.0.5,10.0.0.7,10.0.0.12,10.0.0.19') -> None:

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 ' \
        '--conf spark.cassandra.connection.host=' + cassandra_cluster + ' pyspark-shell'


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(sql_context: context, table_name: str, keys_space_name: str = 'insight') -> dataframe:
    return sql_context.read \
                      .format('org.apache.spark.sql.cassandra') \
                      .options(table=table_name, keyspace=keys_space_name)\
                      .load()


# Writes table_df into Cassandra table table_name
def write_df_to_table(table_df: dataframe, table_name: str, key_space_name: str = 'insight') -> None:
    table_df.write \
            .format('org.apache.spark.sql.cassandra') \
            .mode('append') \
            .options(table=table_name, keyspace=key_space_name) \
            .save()


# Transforms YYYY-MM-DD_hh-mm into YYYY-MM-DD_hh for grouping
def minute_aggregator(timestamp_minute: str) -> str:
    return timestamp_minute[:13] + timestamp_minute[16:]


# Groups by streamer and timestamp, calculates mean of counts
def spark_unified_aggregator(udf_agg: udf, table_df: dataframe, partition_key: str = 'streamer',
                             clustering_key: str = 'timestamp', clustering_key_alias: str = 'timestamp') -> dataframe:

    return table_df.groupBy(partition_key, udf_agg(clustering_key).alias(clustering_key_alias)) \
                   .agg(sql_mean('youtube_count').alias('youtube_count'),
                        sql_mean('twitter_count').alias('twitter_count'),
                        sql_mean('twitch_count').alias('twitch_count'),
                        sql_mean('total_count').alias('total_count')
                        )


if __name__ == '__main__':

    table_to_read = 'unified_minute_view'
    table_to_write = 'unified_hour'

    initialize_environment()

    new_sql_context = create_sql_context('spark_minute_to_hour')
    udf_aggregator = udf(minute_aggregator, StringType())

    hour_table = spark_unified_aggregator(udf_aggregator, load_and_get_table_df(new_sql_context, table_to_read))
    write_df_to_table(hour_table, table_to_write)
