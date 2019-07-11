import findspark
findspark.init()
import os
from pyspark.sql import SQLContext, dataframe, context
from pyspark import SparkContext
from pyspark.sql.functions import mean as sql_mean
from pyspark.sql.functions import sum as sql_sum
from pyspark.sql.functions import udf

"""THIS FILE NEED TO BE PRESENT IN ALL SPARK NODES AT /home/ubuntu/cassandra_jobs/spark_functions.py"""


def create_sql_context(spark_job_name: str, spark_master: str = 'spark://ip-10-0-0-22.ec2.internal:7077') -> context:
    sc = SparkContext(spark_master, spark_job_name)
    sc.addPyFile('/home/ubuntu/cassandra_jobs/spark_functions.py')
    return SQLContext(sc)


# Initializes Cassandra Cluster and Cassandra Driver
def initialize_environment(cassandra_cluster: str = '10.0.0.5,10.0.0.7,10.0.0.12,10.0.0.19') -> None:

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 ' \
        '--conf spark.cassandra.connection.host=' + cassandra_cluster + ' pyspark-shell'


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(spark_sql_context: context, table_name: str, keyspace_name: str = 'insight') -> dataframe:
    return spark_sql_context.read \
                      .format('org.apache.spark.sql.cassandra') \
                      .options(table=table_name, keyspace=keyspace_name) \
                      .load()


# Writes table_df into Cassandra table table_name
def write_df_to_table(table_df: dataframe, table_name: str, key_space_name: str = 'insight') -> None:
    table_df.write \
            .format('org.apache.spark.sql.cassandra') \
            .mode('append') \
            .options(table=table_name, keyspace=key_space_name) \
            .save()


def get_game_aggregation(table_df: dataframe, accounts_df: dataframe) -> dataframe:
    return accounts_df\
        .join(table_df, accounts_df.streamer == table_df.streamer)\
        .select('game', 'timestamp', 'total_count')\
        .groupBy('game', 'timestamp')\
        .agg(sql_sum('total_count').alias('total_count'))


def get_language_aggregation(table_df: dataframe, accounts_df: dataframe) -> dataframe:
    return accounts_df \
        .join(table_df, accounts_df.streamer == table_df.streamer) \
        .select('language', 'timestamp', 'total_count') \
        .groupBy('language', 'timestamp') \
        .agg(sql_sum('total_count').alias('total_count'))


def get_genre_aggregation(table_df: dataframe, accounts_df: dataframe, games_df: dataframe) -> dataframe:
    return accounts_df\
        .join(games_df, accounts_df.game == games_df.game) \
        .join(table_df, accounts_df.streamer == table_df.streamer) \
        .select('genre', 'timestamp', 'total_count') \
        .groupBy('genre', 'timestamp') \
        .agg(sql_sum('total_count').alias('total_count'))


def get_console_aggregation(table_df: dataframe, accounts_df: dataframe, games_df: dataframe) -> dataframe:
    return accounts_df \
        .join(games_df, accounts_df.game == games_df.game) \
        .join(table_df, accounts_df.streamer == table_df.streamer) \
        .select('console', 'timestamp', 'total_count') \
        .groupBy('console', 'timestamp') \
        .agg(sql_sum('total_count').alias('total_count'))


# Transforms YYYY-MM-DD_hh into YYYY-MM-DD for grouping
def hour_aggregator(timestamp_hour: str) -> str:
    return timestamp_hour[:10] + timestamp_hour[13:]


# Transforms YYYY-MM-DD_hh-mm into YYYY-MM-DD_hh for grouping
def minute_aggregator(timestamp_minute: str) -> str:
    return timestamp_minute[:13] + timestamp_minute[16:]


# Transforms YYYY-MM-DD_hh-mm-ss into YYYY-MM-DD_hh-mm for grouping
def second_aggregator(timestamp_second: str) -> str:
    return timestamp_second[:16]+timestamp_second[19:]


# Groups by streamer and timestamp, calculates mean of counts
def spark_live_aggregator(udf_agg: udf, table_df: dataframe, initial_value: str, new_value_alias: str,
                          partition_key: str = 'streamer', clustering_key: str = 'timestamp',
                          clustering_key_alias: str = 'timestamp') -> dataframe:

    return table_df.groupBy(partition_key, udf_agg(clustering_key).alias(clustering_key_alias)) \
                   .agg(sql_mean(initial_value).alias(new_value_alias))


# Groups by streamer and timestamp, calculates mean of counts
def spark_unified_aggregator(udf_agg: udf, table_df: dataframe, partition_key: str = 'streamer',
                             clustering_key: str = 'timestamp', clustering_key_alias: str = 'timestamp') -> dataframe:

    return table_df.groupBy(partition_key, udf_agg(clustering_key).alias(clustering_key_alias)) \
                   .agg(sql_mean('youtube_count').alias('youtube_count'),
                        sql_mean('twitter_count').alias('twitter_count'),
                        sql_mean('twitch_count').alias('twitch_count'),
                        sql_mean('total_count').alias('total_count')
                        )


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

