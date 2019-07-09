from cassandra.cluster import Cluster

cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
cassandra_session = cassandra_cluster.connect('')

print('THIS SCRIPT DROPS KEYSPACE AND ALL TABLES!!!! CAREFUL!!!!')
print('PRESS ENTER TO CONTINUE, OTHERWISE CTRL+C TO STOP!')
input()

cassandra_session.execute('DROP KEYSPACE IF EXISTS insight;')
cassandra_cluster.shutdown()
