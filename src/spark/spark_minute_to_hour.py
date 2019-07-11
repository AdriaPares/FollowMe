import spark_functions as sf
from pyspark.sql.functions import udf, StringType


if __name__ == '__main__':

    table_to_read = 'unified_minute_view'
    table_to_write = 'unified_hour'

    sf.initialize_environment()

    new_sql_context = sf.create_sql_context('spark_minute_to_hour')
    udf_aggregator = udf(sf.minute_aggregator, StringType())

    hour_table = sf.spark_unified_aggregator(udf_aggregator, sf.load_and_get_table_df(new_sql_context, table_to_read))
    sf.write_df_to_table(hour_table, table_to_write)
