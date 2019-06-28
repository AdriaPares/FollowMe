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

time_frames = {0: 'Live', 1: 'Minute', 2: 'Hour', 3: 'Day'}

app.layout = html.Div([
    html.Div([
        dcc.Dropdown(
            id='user',
            options=[{'label': 'elded', 'value': 'elded'}],
            value='elded'
        )
    ], style={'width': '48%', 'display': 'inline-block'}
    ),

    html.Div([
        dcc.Graph(id='media-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1000,
            n_intervals=0
        ),
        dcc.Slider(
            id='time--slider',
            min=0,
            max=3,
            value=3,
            marks=time_frames,
            step=None
    )
    ])
])


@app.callback(Output('media-graph', 'figure'),
              [Input('graph-update', 'n_intervals'),
               Input('user', 'value'),
               Input('time--slider', 'value')])
def update_graph_scatter(n, user, time_frame):

    session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
    session.default_fetch_size = 100000

    if time_frames.get(time_frame, '') == 'Live':
        df_youtube = session.execute(prepared_query_youtube_live, (user,))._current_rows
        df_twitch = session.execute(prepared_query_twitch_live, (user,))._current_rows
        df_twitter = session.execute(prepared_query_twitter_live, (user,))._current_rows
    elif time_frames.get(time_frame, '') == 'Minute':
        df_youtube = session.execute(prepared_query_youtube_minute, (user,))._current_rows
        df_twitch = session.execute(prepared_query_twitch_minute, (user,))._current_rows
        df_twitter = session.execute(prepared_query_twitter_minute, (user,))._current_rows
    elif time_frames.get(time_frame, '') == 'Hour':
        df_youtube = session.execute(prepared_query_youtube_hour, (user,))._current_rows
        df_twitch = session.execute(prepared_query_twitch_hour, (user,))._current_rows
        df_twitter = session.execute(prepared_query_twitter_hour, (user,))._current_rows
    elif time_frames.get(time_frame, '') == 'Day':
        df_youtube = session.execute(prepared_query_youtube_day, (user,))._current_rows
        df_twitch = session.execute(prepared_query_twitch_day, (user,))._current_rows
        df_twitter = session.execute(prepared_query_twitter_day, (user,))._current_rows

    title = user + 'Social Media'

    labels = ['YouTube', 'Twitter', 'Twitch']

    colors = ['rgb(255,0,0)', 'rgb(0,172,237)', 'rgb(100,65,165)']

    mode_size = [8, 8, 8]

    line_size = [2, 2, 2]

    x_data = [df_youtube[x_name], df_twitter[x_name], df_twitch[x_name]]

    y_data = [df_youtube[y_name], df_twitter[y_name], df_twitch[y_name]]

    traces = []

    for i in range(0, 3):
        traces.append(go.Scatter(
            x=x_data[i],
            y=y_data[i],
            mode='lines+markers',
            line=dict(color=colors[i], width=line_size[i]),
            marker=dict(color=colors[i], size=mode_size[i]),
            connectgaps=True,
        ))

    layout = go.Layout(
        xaxis=dict(
            showline=True,
            showgrid=False,
            showticklabels=True,
            linecolor='rgb(204, 204, 204)',
            linewidth=2,
            ticks='outside',
            tickcolor='rgb(204, 204, 204)',
            tickwidth=2,
            ticklen=5,
            tickfont=dict(
                family='Arial',
                size=12,
                color='rgb(82, 82, 82)',
            ),
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            showticklabels=False,
        ),
        autosize=False,
        margin=dict(
            autoexpand=False,
            l=100,
            r=20,
            t=110,
        ),
        showlegend=False
    )

    annotations = []

    # Adding labels
    for y_trace, label, color in zip(y_data, labels, colors):
        # labeling the left_side of the plot
        annotations.append(dict(xref='paper', x=0.05, y=y_trace.iloc[0],
                                xanchor='right', yanchor='middle',
                                text=label + ' {:,}'.format(y_trace.iloc[0]),
                                font=dict(family='Arial',
                                          size=16),
                                showarrow=False))
        # labeling the right_side of the plot
        annotations.append(dict(xref='paper', x=0.95, y=y_trace.iloc[-1],
                                xanchor='left', yanchor='middle',
                                text='{:,}'.format(y_trace.iloc[-1]),
                                font=dict(family='Arial',
                                          size=16),
                                showarrow=False))
    # Title
    annotations.append(dict(xref='paper', yref='paper', x=0.0, y=1.05,
                            xanchor='left', yanchor='bottom',
                            text=user + ' Social Media by Hour',
                            font=dict(family='Arial',
                                      size=30,
                                      color='rgb(37,37,37)'),
                            showarrow=False))

    layout['annotations'] = annotations
    return {'data': traces, 'layout': layout}


if __name__ == '__main__':
    x_name = 'timestamp'
    y_name = 'follower_count'
    cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    session = cluster.connect()
    session.set_keyspace('insight')
    prepared_query_youtube_day = session.prepare("select timestamp, follower_count from youtube_day "
                                                 "where streamer=?");
    prepared_query_twitter_day = session.prepare("select timestamp, follower_count from twitter_day "
                                                 "where streamer=?");
    prepared_query_twitch_day = session.prepare("select timestamp, follower_count from twitch_day "
                                                "where streamer=?");
    prepared_query_youtube_hour = session.prepare("select timestamp, follower_count from youtube_hour "
                                                  "where streamer=?");
    prepared_query_twitter_hour = session.prepare("select timestamp, follower_count from twitter_hour "
                                                  "where streamer=?");
    prepared_query_twitch_hour = session.prepare("select timestamp, follower_count from twitch_hour "
                                                 "where streamer=?");
    prepared_query_youtube_minute = session.prepare("select timestamp, follower_count from youtube_minute "
                                                    "where streamer=?");
    prepared_query_twitter_minute = session.prepare("select timestamp, follower_count from twitter_minute "
                                                  "where streamer=?");
    prepared_query_twitch_minute = session.prepare("select timestamp, follower_count from twitch_minute "
                                                 "where streamer=?");
    prepared_query_youtube_live = session.prepare("select timestamp, follower_count from youtube_live "
                                                  "where streamer=?");
    prepared_query_twitter_live = session.prepare("select timestamp, follower_count from twitter_live "
                                                  "where streamer=?");
    prepared_query_twitch_live = session.prepare("select timestamp, follower_count from twitch_live "
                                                 "where streamer=?");
    app.run_server(debug=True, host='0.0.0.0', port=5000)
