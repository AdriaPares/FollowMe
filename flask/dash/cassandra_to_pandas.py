from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider
import pandas as pd


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
session = cluster.connect()
session.set_keyspace('insight')
session.row_factory = pandas_factory
session.default_fetch_size = 100000000
prepared_query = session.prepare("select timestamp, follower_count from youtube_hour "
                                 "where streamer=?");
rows = session.execute(prepared_query, ('elded',))
print(type(rows))
df = rows._current_rows
print(df.head())
