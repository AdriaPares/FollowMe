from cassandra.cluster import Cluster, Session


# Returns List of tables to create
def get_cassandra_tables() -> list:
    tables_to_create = []
    create_table = 'create table if not exists insight.'
    live_columns = ' (streamer text, timestamp text, follower_count int, primary key (streamer, timestamp));'
    live_tables = ['twitch_live', 'twitter_live', 'youtube_live']

    unified_columns = ' (streamer text, timestamp text, youtube_count int, twitter_count int, twitch_count int, ' \
                      'total_count bigint, primary key (streamer, timestamp));'
    unified_tables = ['unified_minute', 'unified_hour', 'unified_day']

    # Create live tables
    for live_table in live_tables:
        tables_to_create.append(create_table + live_table + live_columns)

    # Unified tables
    for unified_table in unified_tables:
        tables_to_create.append(create_table + unified_table + unified_columns)

    # Aggregations
    tables_to_create.append('create table if not exists insight.language_aggregation '
                            '(language text, timestamp text, total_count bigint, primary key (timestamp, language));')
    tables_to_create.append('create table if not exists insight.game_aggregation '
                            '(game text, timestamp text, total_count bigint, primary key (timestamp, game));')
    tables_to_create.append('create table if not exists insight.genre_aggregation '
                            '(genre text, timestamp text, total_count bigint, primary key (timestamp, genre));')
    tables_to_create.append('create table if not exists insight.console_aggregation '
                            '(console text, timestamp text, total_count bigint, primary key (timestamp, console));')
    # Accounts info
    tables_to_create.append('create table if not exists insight.accounts (streamer text primary key, '
                            'language text, game text);')
    # Games info
    tables_to_create.append('create table if not exists insight.games '
                            '(game text primary key, genre text, console text);')

    return tables_to_create


# Create Keyspace
def create_keyspace(session: Session, keyspace: str = 'insight') -> None:
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                    " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};")


# Create Tables
# Tables are partitioned by streamer and clustered by timestamp
def create_tables(tables: list, session: Session) -> None:
    for table in tables:
        session.execute(table)


def main():
    cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    cassandra_session = cassandra_cluster.connect('')
    cassandra_tables = get_cassandra_tables()
    create_keyspace(cassandra_session)
    create_tables(cassandra_tables, cassandra_session)
    cassandra_cluster.shutdown()


if __name__ == '__main__':
    main()
