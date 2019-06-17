#import uuid
from flask import Flask
from flask_cassandra import CassandraCluster

app = Flask(__name__)
cassandra = CassandraCluster()

app.config['CASSANDRA_NODES'] = ['10.0.0.4','10.0.0.5','10.0.0.6','10.0.0.14']  # can be a string or list of nodes

@app.route('/cassandra_test')
def cassandra_test():
    session = cassandra.connect()
    session.set_keyspace('tutorialspoint')
    cql = "SELECT * FROM avgs LIMIT 1"
    r = session.execute(cql)
    return str(r[0])

if __name__ == '__main__':
    app.run()
