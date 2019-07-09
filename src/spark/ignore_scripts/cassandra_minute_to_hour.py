# Configuration related to Cassandra connector & Cluster
import findspark
findspark.init()

import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import udf, StringType
from pyspark.sql.functions import mean as sql_mean
import spark_functions


spark_job = []
table_names = []
initialize_environment()
new_sql_context = create_sql_context(spark_job)
original_table = load_and_get_table_df(new_sql_context, table_names)
new_table = spark_aggregator(aggregator, original_table, table_name, table_name)
write_df_to_table(new_table, table_names)




udf_minute_aggregator = udf(minute_aggregator, StringType())
minutes = load_and_get_table_df('insight', 'twitch_minute')
hours = minutes.groupBy(udf_minute_aggregator('timestamp_name').alias('timestamp_name'))\
                          .agg(sql_mean('follower_count').alias('follower_count'))
hours.show()
hours.write\
    .format('org.apache.spark.sql.cassandra')\
    .mode('append')\
    .options(table='twitch_hour', keyspace='insight')\
    .save()

sc.stop()






