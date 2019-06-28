import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
import pandas as pd
from collections import deque
from cassandra.cluster import Cluster
import datetime
import time
from json import loads


app = dash.Dash(__name__)
app.layout = html.Div(
    [
        dcc.Dropdown(
            id='user',
            options=[{'label': 'elded', 'value': 'elded'}],
            value='elded'
        ),
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1000,
            n_intervals=0
        ),
    ]
)


@app.callback(Output('live-graph', 'figure'),
              [Input('graph-update', 'n_intervals'),
               Input('user', 'value')])
def update_graph_scatter(n, user):

    session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
    session.default_fetch_size = 100000
    prepared_query_y = session.prepare("select timestamp, follower_count from youtube_hour "
                                     "where streamer=?");
    rows = session.execute(prepared_query_y, (user,))
    df = rows._current_rows
    # print(user)
    # print(df.head())
    data = plotly.graph_objs.Scatter(
            x=df['timestamp'],
            y=df['follower_count'],
            name='Scatter',
            mode='lines+markers'
            )

    return {'data': [data], 'layout':
            go.Layout(autosize=True)}
                      # ,xaxis=dict(range=[df['timestamp'].min(), df['timestamp'].max()]),
                      # yaxis=dict(range=[df['follower_count'].min(), df['follower_count'].max()]))}


if __name__ == '__main__':
    cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    session = cluster.connect()
    session.set_keyspace('insight')
    app.run_server(debug=True, host='0.0.0.0', port=5000)
