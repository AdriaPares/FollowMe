from cassandra.cluster import Cluster

cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
cassandra_session = cassandra_cluster.connect('')


def create_table_queries():
    queries = dict(create_youtube_live="create table if not exists insight.youtube_live "
                                       "(timestamp_name text primary key, subscriber_count int) "
                                       "with default_time_to_live=120;",
                   create_youtube_minute="create table if not exists insight.youtube_minute "
                                         "(timestamp_name text primary key, subscriber_count int) "
                                         "with default_time_to_live=7200;",
                   create_youtube_hour="create table if not exists insight.youtube_hour "
                                       "(timestamp_name text primary key, subscriber_count int) "
                                       "with default_time_to_live=172800;",
                   create_youtube_day="create table if not exists insight.youtube_day "
                                      "(timestamp_name text primary key, subscriber_count int) ;",


                   create_twitch_live="create table if not exists insight.twitch_live "
                                      "(timestamp_name text primary key, follower_count int) "
                                      "with default_time_to_live=120;",
                   create_twitch_minute="create table if not exists insight.twitch_minute "
                                        "(timestamp_name text primary key, follower_count int) "
                                        "with default_time_to_live=7200;",
                   create_twitch_hour="create table if not exists insight.twitch_hour "
                                      "(timestamp_name text primary key, follower_count int) "
                                      "with default_time_to_live=172800;",
                   create_twitch_day="create table if not exists insight.twitch_day "
                                     "(timestamp_name text primary key, follower_count int) ;",


                   create_twitter_live="create table if not exists insight.twitter_live "
                                       "(timestamp_name text primary key, follower_count int) "
                                       "with default_time_to_live = 120;",
                   create_twitter_minute="create table if not exists insight.twitter_minute "
                                         "(timestamp_name text primary key, follower_count int) "
                                         "with default_time_to_live = 7200;",
                   create_twitter_hour="create table if not exists insight.twitter_hour "
                                       "(timestamp_name text primary key, follower_count int) "
                                       "with default_time_to_live = 172800;",
                   create_twitter_day="create table if not exists insight.twitter_day "
                                      "(timestamp_name text primary key, follower_count int) ;")
    return queries


cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS insight "
                          "WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};")
for query_name, query in create_table_queries().items():
    cassandra_session.execute(query)
cassandra_cluster.shutdown()
