from cassandra.cluster import Cluster

cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
cassandra_session = cassandra_cluster.connect('')

create_table_query = 'create table if not exists insight.'
create_ledger_columns = '(streamer text, timestamp text, follower_count int, primary key (streamer, timestamp)) '
create_trend_columns = '(streamer text, timestamp text, trend int, primary key (streamer, timestamp)) '
platforms = ['twitch', 'twitter', 'youtube']
time_frames = {'_live ': 'with default_time_to_live=120;',
               '_minute ': 'with default_time_to_live=7200;',
               '_hour ': 'with default_time_to_live=172800;',
               '_day ': ';'}

# Create Keyspace
cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS insight "
                          "WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};")

for platform in platforms:
    # Create Trend tables
    cassandra_session.execute(create_table_query + platform + '_trend ' + create_trend_columns)
    for time, time_to_live in time_frames.items():
        # Create Record tables
        cassandra_session.execute(create_table_query + platform + time + create_ledger_columns + time_to_live)

# Create Account metadata tables

# Accounts
cassandra_session.execute('create table if not exists insight.accounts (streamer text primary key, '
                          'language text, game text);')
# Games
cassandra_session.execute('create table if not exists insight.games (game text primary key, genre text, console text);')

# Aggregations
cassandra_session.execute('create table if not exists insight.language_aggregation '
                          '(language text, timestamp text, total_count int, primary key (language, timestamp)')
cassandra_session.execute('create table if not exists insight.game_aggregation '
                          '(game text, timestamp text, total_count int, primary key (language, timestamp)')
cassandra_session.execute('create table if not exists insight.genre_aggregation '
                          '(genre text, timestamp text, total_count int, primary key (language, timestamp)')
cassandra_session.execute('create table if not exists insight.console_aggregation '
                          '(console text, timestamp text, total_count int, primary key (language, timestamp)')

cassandra_cluster.shutdown()
