# Configuration related to Cassandra connector & Cluster
import findspark
findspark.init()

import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import udf, StringType
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import mean as sql_mean

# Unknown if this needs to run every time we call this...
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 ' \
                                    '--conf spark.cassandra.connection.host=10.0.0.4,' \
                                    '10.0.0.5,10.0.0.6,10.0.0.14 pyspark-shell'

sc = SparkContext('local', 'cassandra_live_to_minute')
# Creating PySpark SQL Context

sqlContext = SQLContext(sc)


# Loads and returns data frame for a table including key space given
def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format('org.apache.spark.sql.cassandra')\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df


def hour_aggregator(timestamp_second):
    return timestamp_second[:10]+timestamp_second[13:]


udf_hour_aggregator = udf(hour_aggregator, StringType())
seconds = load_and_get_table_df('insight', 'twitch_hour')
minutes = seconds.groupBy(udf_hour_aggregator('timestamp_name').alias('timestamp_name'))\
                          .agg(sql_mean('follower_count').alias('follower_count'))
minutes.show()
minutes.write\
    .format('org.apache.spark.sql.cassandra')\
    .mode('append')\
    .options(table='twitch_day', keyspace='insight')\
    .save()

