"""This script aggregates metadata: follower count by language, game, genre and console"""

import spark_functions as sf


if __name__ == '__main__':

    job_name = 'metadata_aggregator'
    table_to_read = 'unified_day'
    tables_to_write = ['console_aggregation', 'language_aggregation', 'game_aggregation', 'genre_aggregation']

    sf.initialize_environment()
    sql_context = sf.create_sql_context(job_name)

    # Unified day Data Frame
    unified_day = sf.load_and_get_table_df(sql_context, table_to_read)

    # Games Data Frame
    games_data_frame = sf.load_and_get_table_df(sql_context, 'games')

    # Accounts Data Frame
    accounts_data_frame = sf.load_and_get_table_df(sql_context, 'accounts')

    # Aggregations

    # Game aggregation
    game_aggregation = sf.get_game_aggregation(unified_day, accounts_data_frame)
    sf.write_df_to_table(game_aggregation, 'game_aggregation')

    # Language aggregation

    language_aggregation = sf.get_language_aggregation(unified_day, accounts_data_frame)
    sf.write_df_to_table(language_aggregation, 'language_aggregation')

    # Genre aggregation

    genre_aggregation = sf.get_genre_aggregation(unified_day, accounts_data_frame, games_data_frame)
    sf.write_df_to_table(genre_aggregation, 'genre_aggregation')

    # Console aggregation

    console_aggregation = sf.get_console_aggregation(unified_day, accounts_data_frame, games_data_frame)
    sf.write_df_to_table(console_aggregation, 'console_aggregation')
