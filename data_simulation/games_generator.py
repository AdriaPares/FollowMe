# Takes a JSON with the games info file and writes to Cassandra Cluster
# To be run in Cassandra Cluster
# DON'T RUN IN LOCAL

from cassandra.cluster import Cluster
import json

cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
cassandra_session = cassandra_cluster.connect('insight')
games_prep = cassandra_session.prepare("insert into games (game, genre, platform) values (?,?,?)");

with open('games_info.json') as f:
    games = json.load(f)

for game, attributes in games:
    cassandra_session.execute(games_prep, [game, attributes['genre'], attributes['platform']])
print('GAMES DONE.')
cassandra_cluster.shutdown()
