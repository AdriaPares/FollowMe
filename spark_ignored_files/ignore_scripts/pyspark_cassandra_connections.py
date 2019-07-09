# Configuratins related to Cassandra connector & Cluster
import os

#Unknown if this needs to run everytime we call this...
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=10.0.0.4,10.0.0.5,10.0.0.6,10.0.0.14 pyspark-shell'

from pyspark import SparkContext
from pyspark.sql.functions import lit
sc = SparkContext("local", "twitch_test")

# Creating PySpark SQL Context
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# Loads and returns data frame for a table including key space given
def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df

# Loading movies & ratings table data frames
seconds = load_and_get_table_df("tutorialspoint", "kafkatest")

#seconds.show()

minutes = seconds.agg({'subs':'avg'})

minutes = minutes.withColumn('avg_name',lit('first average'))\
    .withColumnRenamed('avg(subs)','avg_value')\
    .select('avg_name','avg_value')

minutes.show()
minutes.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="avgs", keyspace="tutorialspoint")\
    .save()

