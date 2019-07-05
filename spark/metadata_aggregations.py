# Configuration related to Cassandra connector & Cluster
# This script aggregates metadata: follower count by language, game, genre and console
import findspark
findspark.init()
import os
from pyspark.sql import SQLContext, context, dataframe
from pyspark import SparkContext
from pyspark.sql.functions import udf, StringType
from pyspark.sql.functions import mean as sql_mean
from pyspark.sql.functions import sum as sql_sum


def create_sql_context(spark_job_name: str, spark_master: str = 'spark://ip-10-0-0-22.ec2.internal:7077') -> context:
    sc = SparkContext(spark_master, spark_job_name)
    return SQLContext(sc)


# Initializes Cassandra Cluster and Cassandra Driver
def initialize_environment(cassandra_cluster: str = '10.0.0.5,10.0.0.7,10.0.0.12,10.0.0.19') -> None:

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 ' \
        '--conf spark.cassandra.connection.host=' + cassandra_cluster + ' pyspark-shell'


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(spark_sql_context: context, table_name: str, keys_space_name: str = 'insight') -> dataframe:
    return spark_sql_context.read \
                      .format('org.apache.spark.sql.cassandra') \
                      .options(table=table_name, keyspace=keys_space_name) \
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
    return accounts_data_frame \
        .join(games_df, accounts_df.game == games_df.game) \
        .join(table_df, accounts_df.streamer == table_df.streamer) \
        .select('console', 'timestamp', 'total_count') \
        .groupBy('console', 'timestamp') \
        .agg(sql_sum('total_count').alias('total_count'))


if __name__ == '__main__':

    job_name = 'metadata_aggregator'
    table_to_read = 'unified_day'
    tables_to_write = ['console_aggregation', 'language_aggregation', 'game_aggregation', 'genre_aggregation']

    initialize_environment()
    sql_context = create_sql_context(job_name)

    unified_day = load_and_get_table_df(sql_context, table_to_read)

    # Games Data Frame
    games_data_frame = load_and_get_table_df(sql_context, 'games')

    # Accounts Data Frame
    accounts_data_frame = load_and_get_table_df(sql_context, 'accounts')

    # Aggregations

    # Game aggregation
    game_aggregation = get_game_aggregation(unified_day, accounts_data_frame)
    write_df_to_table(game_aggregation, 'game_aggregation')

    # Language aggregation

    language_aggregation = get_language_aggregation(unified_day, accounts_data_frame)
    write_df_to_table(language_aggregation, 'language_aggregation')

    # Genre aggregation

    genre_aggregation = get_genre_aggregation(unified_day, accounts_data_frame, games_data_frame)
    write_df_to_table(genre_aggregation, 'genre_aggregation')

    # Console aggregation

    console_aggregation = get_console_aggregation(unified_day, accounts_data_frame, games_data_frame)
    write_df_to_table(console_aggregation, 'console_aggregation')
