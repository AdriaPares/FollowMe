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


# Transforms YYYY-MM-DD_hh-mm-ss into YYYY-MM-DD_hh-mm for grouping
def second_aggregator(timestamp_second: str) -> str:
    return timestamp_second[:16]+timestamp_second[19:]


# Groups by streamer and timestamp, calculates mean of counts
def spark_aggregator(udf_agg: udf, table_df: dataframe, initial_value: str, new_value_alias: str,
                     partition_key: str = 'streamer', clustering_key: str = 'timestamp',
                     clustering_key_alias: str = 'timestamp') -> dataframe:

    return table_df.groupBy(partition_key, udf_agg(clustering_key).alias(clustering_key_alias)) \
                   .agg(sql_mean(initial_value).alias(new_value_alias))


# We assume the following schema: streamer, timestamp, value_(1,2,3) for tables 1, 2, 3.
# We join by streamer and timestamp
# The resulting schema is (streamer, timestamp, value_1_alias, value_2_alias, value_3_alias)
def join_3_tables_by_streamer_and_timestamp(
        table_1: dataframe, table_2: dataframe, table_3: dataframe,
        table_1_alias: str = 'youtube_', table_2_alias: str = 'twitch_', table_3_alias: str = 'twitter_'
) -> dataframe:
    table_1 = table_1.withColumnRenamed('follower_count', table_1_alias + 'count')

    table_2 = table_2\
        .withColumnRenamed('streamer', table_2_alias + 'streamer')\
        .withColumnRenamed('timestamp', table_2_alias + 'timestamp')\
        .withColumnRenamed('follower_count', table_2_alias + 'count')

    table_3 = table_3\
        .withColumnRenamed('streamer', table_3_alias + 'streamer')\
        .withColumnRenamed('timestamp', table_3_alias + 'timestamp')\
        .withColumnRenamed('follower_count', table_3_alias + 'count')

    joined_tables = table_1\
        .join(table_2,
              (table_1['streamer'] == table_2[table_2_alias + 'streamer'])
              & (table_1['timestamp'] == table_2[table_2_alias + 'timestamp']))\
        .join(table_3,
              (table_1['streamer'] == table_3[table_3_alias + 'streamer'])
              & (table_1['timestamp'] == table_3[table_3_alias + 'timestamp']))

    return joined_tables.select('streamer', 'timestamp', table_1_alias + 'count',
                                table_2_alias + 'count', table_3_alias + 'count')


if __name__ == '__main__':

    table_to_write = 'unified_minute'
    value_name = 'follower_count'
    value_alias = 'follower_count'

    initialize_environment()

    new_sql_context = create_sql_context('spark_live_to_minute')
    udf_aggregator = udf(second_aggregator, StringType())

    # Get live tables and aggregate into minutes

    youtube_minute = spark_aggregator(udf_aggregator, load_and_get_table_df(new_sql_context, 'youtube_live_view'),
                                      value_name, value_alias)
    twitch_minute = spark_aggregator(udf_aggregator, load_and_get_table_df(new_sql_context, 'twitch_live_view'),
                                     value_name, value_alias)
    twitter_minute = spark_aggregator(udf_aggregator, load_and_get_table_df(new_sql_context, 'twitter_live_view'),
                                      value_name, value_alias)

    joined_minute_tables = join_3_tables_by_streamer_and_timestamp(youtube_minute, twitch_minute, twitter_minute)
    write_df_to_table(joined_minute_tables
                      .withColumn('total_count', col('youtube_count') + col('twitch_count') + col('twitter_count')),
                      table_to_write)
