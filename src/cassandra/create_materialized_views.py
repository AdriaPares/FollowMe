from cassandra.cluster import Cluster, Session
import datetime as dt


# Views are partitioned by timestamp and clustered by streamer
def get_views(day: str = (dt.datetime.today()-dt.timedelta(days=1)).strftime('%Y-%m-%d'),
              last_month: str = (dt.datetime.today() - dt.timedelta(weeks=4)).strftime('%Y-%m-%d')) -> list:
    views = []
    tables_to_view = ['twitch_live', 'twitter_live', 'youtube_live', 'unified_minute', 'unified_hour']
    for table_to_view in tables_to_view:
        views.append("create MATERIALIZED VIEW if not exists " + table_to_view + "_view as select * from "
                     + table_to_view + " where streamer is not null and timestamp > '"
                     + day + "' primary key (timestamp, streamer) ;")

    # Unified day requires the last month for plotting in Dash
    views.append("create MATERIALIZED VIEW if not exists unified_day_view as select * from unified_day where streamer "
                 "is not null and timestamp > '" + last_month + "' primary key (timestamp, streamer) ;")
    return views


# Creates materialized views
def create_views(session: Session, views: list) -> None:
    for view in views:
        session.execute(view)


def main():
    cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    cassandra_session = cassandra_cluster.connect('insight')
    views = get_views()
    create_views(cassandra_session, views)
    cassandra_cluster.shutdown()


if __name__ == '__main__':
    main()
