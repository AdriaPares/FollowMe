import spark_functions as sf
from pyspark.sql.functions import udf, StringType


if __name__ == '__main__':

    table_to_read = 'unified_hour_view'
    table_to_write = 'unified_day'

    sf.initialize_environment()

    new_sql_context = sf.create_sql_context('spark_hour_to_day')
    new_sql_context.addPyFile('./spark_functions.py')
    udf_aggregator = udf(sf.hour_aggregator, StringType())

    hour_table = sf.spark_unified_aggregator(udf_aggregator, sf.load_and_get_table_df(new_sql_context, table_to_read))
    sf.write_df_to_table(hour_table, table_to_write)
