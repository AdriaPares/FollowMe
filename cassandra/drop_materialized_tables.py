from cassandra.cluster import Cluster, Session


# Drops materialized views
def drop_views(session: Session) -> None:
    tables_to_drop = ['twitch_live_view', 'twitter_live_view', 'youtube_live_view',
                      'unified_minute_view', 'unified_hour_view', 'unified_day_view']
    for table_to_drop in tables_to_drop:
        session.execute("DROP MATERIALIZED VIEW " + table_to_drop + ' ;')


def main():
    cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    cassandra_session = cassandra_cluster.connect('insight')
    drop_views(cassandra_session)
    cassandra_cluster.shutdown()


if __name__ == '__main__':
    main()
