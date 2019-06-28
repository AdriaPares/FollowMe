# #import uuid
# from flask import Flask
# from flask_cassandra import CassandraCluster
#
# app = Flask(__name__)

from pystream import app
from flask import render_template
from flask_cassandra import CassandraCluster
# from flaskexample import app
# from sqlalchemy import create_engine
# from sqlalchemy_utils import database_exists, create_database
# import pandas as pd
# import psycopg2

# # PostgreSQL configuration details
# user = 'postgres'
#
#
# host = 'ec2-34-204-179-83.compute-1.amazonaws.com'
# port = '5432'
# dbname = 'mycelias'
# password = 'postgres'
#
# con = None
# con = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password)


cassandra = CassandraCluster()
app.config['CASSANDRA_NODES'] = ['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19']



@app.route('/')
@app.route('/index')
def index():
    return render_template("new_index.html", title='Home', user={'nickname': 'Miguel'})


@app.route('/db')
def cluster_page():
    session = cassandra.connect()
    session.set_keyspace('insight')
    cql = "SELECT * FROM twitter_live LIMIT 1"
    r = session.execute(cql)
    return str(r[0])




    #
    # sql_query = """
    #             SELECT * FROM cluster100k WHERE component='36';
    #             """
    # query_results = pd.read_sql_query(sql_query, con)
    #
    # addresses = ""
    # # get first 20 results
    # for i in range(0, 20):
    #     addresses += query_results.iloc[i]['id']
    #     addresses += "<br>"
    # return addresses


@app.route('/db_fancy')
def cluster_page_fancy():
    sql_query = """
               SELECT * FROM cluster100k WHERE component='32';
                """
    query_results = pd.read_sql_query(sql_query, con)

    statistics = []
    statistics.append(dict(label='Number of addresses', value=query_results.shape[0]))

    addresses = []
    for i in range(0, query_results.shape[0]):
        addresses.append(dict(number=i+1, address=query_results.iloc[i]['id'],
                           component=query_results.iloc[i]['component']))

    return render_template('new_index.html', addresses=addresses, statistics=statistics)








@app.route('/')
@app.route('/index')
def index():
    user = {'username': 'Miguel'}
    posts = [
        {
            'author': {'username': 'John'},
            'body': 'Beautiful day in Portland!'
        },
        {
            'author': {'username': 'Susan'},
            'body': 'The Avengers movie was so cool!'
        }
    ]
    return render_template('index.html', title='Home', user=user, posts=posts)


# app.config['CASSANDRA_NODES'] = ['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19']
#
#
# @app.route('/cassandra_test')
# def cassandra_test():
#     session = cassandra.connect()
#     session.set_keyspace('insight')
#     cql = "SELECT * FROM twitter_live LIMIT 1"
#     r = session.execute(cql)
#     return str(r[0])
#
#
# if __name__ == '__main__':
#     app.run()
