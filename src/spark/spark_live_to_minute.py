import spark_functions as sf
from pyspark.sql.functions import udf, StringType, col


if __name__ == '__main__':

    table_to_write = 'unified_minute'
    value_name = 'follower_count'
    value_alias = 'follower_count'

    sf.initialize_environment()

    new_sql_context = sf.create_sql_context('spark_live_to_minute')
    udf_aggregator = udf(sf.second_aggregator, StringType())

    # Get live tables and aggregate into minutes

    youtube_minute = sf.spark_live_aggregator(udf_aggregator, sf.load_and_get_table_df(new_sql_context, 'youtube_live_view'),
                                              value_name, value_alias)
    twitch_minute = sf.spark_live_aggregator(udf_aggregator, sf.load_and_get_table_df(new_sql_context, 'twitch_live_view'),
                                             value_name, value_alias)
    twitter_minute = sf.spark_live_aggregator(udf_aggregator, sf.load_and_get_table_df(new_sql_context, 'twitter_live_view'),
                                              value_name, value_alias)

    joined_minute_tables = sf.join_3_tables_by_streamer_and_timestamp(youtube_minute, twitch_minute, twitter_minute)
    sf.write_df_to_table(joined_minute_tables
                         .withColumn('total_count', col('youtube_count') + col('twitch_count') + col('twitter_count')),
                         table_to_write)
