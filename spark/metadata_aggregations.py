# Configuration related to Cassandra connector & Cluster
# This script aggregates metadata: follower count by language, game, genre and console
import findspark
findspark.init()
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import udf, StringType
from pyspark.sql.functions import mean as sql_mean
from pyspark.sql.functions import sum as sql_sum


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


def spark_aggregator(udf_agg, table_df, initial_value, new_value_alias, partition_key='streamer',
                     clustering_key='timestamp', clustering_key_alias='timestamp'):

    return table_df.groupBy(partition_key, udf_agg(clustering_key).alias(clustering_key_alias)) \
                   .agg(sql_mean(initial_value).alias(new_value_alias))


job_name = 'metadata_aggregator'
tables_to_read = ['twitch_day', 'twitter_day', 'youtube_day']
tables_to_write = ['console_aggregation', 'language_aggregation', 'game_aggregation', 'genre_aggregation']
key_to_write = ['console', 'language', 'game', 'genre']
value_to_read = 'follower_count'
value_to_read_alias = 'follower_count'
value_to_write = 'total_count'

table_partition_key = 'streamer'
table_clustering_key = 'timestamp'

initialize_environment()
sql_context = create_sql_context(job_name)

# Union of all platforms, grouped by streamer and timestamp, sum of follower_count

#######
# Fix this: it should start with an empty dataframe and run over all tables doing unions

platform_union = load_and_get_table_df(sql_context, tables_to_read[0])\
    .union(load_and_get_table_df(sql_context, tables_to_read[1]))\
    .union(load_and_get_table_df(sql_context, tables_to_read[2]))\
    .groupBy(table_partition_key, table_clustering_key)\
    .agg(sql_sum(value_to_read).alias(value_to_read_alias))


#######

# union_testing = sql_context.createDataFrame(y_df).union(sqlContext.createDataFrame(tw_df))\
# .union(sqlContext.createDataFrame(th_df)).groupBy('streamer', 'timestamp')\
# .agg(sql_sum('follower_count').alias('follower_count'))


# Games Data Frame
games_data_frame = load_and_get_table_df(sql_context, 'games')

# Accounts Data Frame
accounts_data_frame = load_and_get_table_df(sql_context, 'accounts')

# Aggregations
# Output schema: aggregation, timestamp YYYY-MM-DD, total_count

# Game aggregation

game_aggregation = accounts_data_frame\
    .join(platform_union, accounts_data_frame.streamer == platform_union.streamer)\
    .select('game', 'timestamp', 'follower_count')\
    .groupBy('game', 'timestamp')\
    .agg(sql_sum('follower_count').alias('total_count'))

write_df_to_table(game_aggregation, 'game_aggregation')

# Language aggregation

language_aggregation = accounts_data_frame\
    .join(platform_union, accounts_data_frame.streamer == platform_union.streamer)\
    .select('language', 'timestamp', 'follower_count')\
    .groupBy('language', 'timestamp')\
    .agg(sql_sum('follower_count').alias('total_count'))

write_df_to_table(language_aggregation, 'language_aggregation')

# Genre aggregation

genre_aggregation = accounts_data_frame\
    .join(games_data_frame, accounts_data_frame.game == games_data_frame.game)\
    .join(platform_union, accounts_data_frame.streamer == platform_union.streamer)\
    .select('genre', 'timestamp', 'follower_count')\
    .groupBy('genre', 'timestamp')\
    .agg(sql_sum('follower_count').alias('total_count'))

write_df_to_table(genre_aggregation, 'genre_aggregation')

# Console aggregation

console_aggregation = accounts_data_frame\
    .join(games_data_frame, accounts_data_frame.game == games_data_frame.game)\
    .join(platform_union, accounts_data_frame.streamer == platform_union.streamer)\
    .select('console', 'timestamp', 'follower_count')\
    .groupBy('console', 'timestamp')\
    .agg(sql_sum('follower_count').alias('total_count'))

write_df_to_table(console_aggregation, 'console_aggregation')
