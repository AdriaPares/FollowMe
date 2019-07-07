"""Create a Dash app within a Flask app."""
import glob
from pathlib import Path
from dash import Dash
import dash_table
import dash_html_components as html
import pandas as pd
from .layout import html_layout
import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
# import pandas as pd
from collections import deque
from cassandra.cluster import Cluster
import datetime
import time
from json import loads
import dash_bootstrap_components as dbc


def Add_Dash(server):
    """Create a Dash app."""
    external_stylesheets = ['/static/dist/css/styles.css',
                            'https://fonts.googleapis.com/css?family=Lato',
                            'https://use.fontawesome.com/releases/v5.8.1/css/all.css']
    external_scripts = ['/static/dist/js/includes/jquery.min.js',
                        '/static/dist/js/main.js']
    dash_app = Dash(server=server,
                    external_stylesheets=external_stylesheets,
                    external_scripts=external_scripts,
                    routes_pathname_prefix='/trends/')

    # Override the underlying HTML template
    dash_app.index_string = html_layout

    # Add Cassandra Queries and Parameters
    # Pass this as a dictionary of parameters, hidden in a function

    x_name = 'timestamp'
    y_name = 'follower_count'
    cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    session = cluster.connect()
    session.set_keyspace('insight')

    # prepared_query_youtube_day = session.prepare("select timestamp, follower_count from youtube_day "
    #                                              "where streamer=?")
    # prepared_query_twitter_day = session.prepare("select timestamp, follower_count from twitter_day "
    #                                              "where streamer=?")
    # prepared_query_twitch_day = session.prepare("select timestamp, follower_count from twitch_day "
    #                                             "where streamer=?")
    # prepared_query_youtube_hour = session.prepare("select timestamp, follower_count from youtube_hour "
    #                                               "where streamer=?")
    # prepared_query_twitter_hour = session.prepare("select timestamp, follower_count from twitter_hour "
    #                                               "where streamer=?")
    # prepared_query_twitch_hour = session.prepare("select timestamp, follower_count from twitch_hour "
    #                                              "where streamer=?")
    # prepared_query_youtube_minute = session.prepare("select timestamp, follower_count from youtube_minute "
    #                                                 "where streamer=?")
    # prepared_query_twitter_minute = session.prepare("select timestamp, follower_count from twitter_minute "
    #                                                 "where streamer=?")
    # prepared_query_twitch_minute = session.prepare("select timestamp, follower_count from twitch_minute "
    #                                                "where streamer=?")
    prepared_query_youtube_live = session.prepare("select timestamp, follower_count from youtube_live "
                                                  "where streamer=?")
    prepared_query_twitter_live = session.prepare("select timestamp, follower_count from twitter_live "
                                                  "where streamer=?")
    prepared_query_twitch_live = session.prepare("select timestamp, follower_count from twitch_live "
                                                 "where streamer=?")

    prepared_query_day = session.prepare("select timestamp, twitch_count, twitter_count, youtube_count, total_count "
                                         "from unified_day where streamer=?")
    prepared_query_hour = session.prepare("select timestamp, twitch_count, twitter_count, youtube_count, total_count "
                                          "from unified_hour where streamer=?")
    prepared_query_minute = session.prepare("select timestamp, twitch_count, twitter_count, youtube_count, total_count "
                                            "from unified_minute where streamer=?")

    prepared_query_accounts = session.prepare("select streamer from insight.accounts;")

    # Get streamers names, and prepare the dropdown options
    session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
    session.default_fetch_size = 100000

    streamers = session.execute(prepared_query_accounts)._current_rows
    # print(streamers)

    dropdown_options = [{'label': streamer, 'value': streamer} for streamer in streamers.streamer]
    # print(dropdown_options)

    # Create Dash Layout comprised of Graphs
    dash_app.layout = html.Div(
        children=get_followers(dropdown_options),
        id='dash-container'
    )

    # Initialize callbacks after our app is loaded
    # Pass dash_app as a parameter

    init_callbacks(dash_app, session, x_name, y_name,
                   # prepared_query_youtube_day, prepared_query_twitter_day, prepared_query_twitch_day,
                   # prepared_query_youtube_hour, prepared_query_twitter_hour, prepared_query_twitch_hour,
                   # prepared_query_youtube_minute, prepared_query_twitter_minute, prepared_query_twitch_minute,
                   prepared_query_day, prepared_query_hour, prepared_query_minute,
                   prepared_query_youtube_live, prepared_query_twitter_live, prepared_query_twitch_live)

    return dash_app.server


def get_followers(dropdown_options):
    """Plots 4 charts with live, minute, hour and daily follower counts"""
    layout = html.Div([
        dbc.Row(
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='user',
                        options=dropdown_options,
                        value='ninja'
                    )
                ]
                )
            )
        ),
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.Graph(id='hour-graph', animate=True),
                    dcc.Interval(
                        id='hour-update',
                        interval=1000 * 60 * 60,
                        n_intervals=0
                    )
                ]
                )
            ),
            dbc.Col(
                html.Div([
                    dcc.Graph(id='day-graph', animate=True),
                    dcc.Interval(
                        id='day-update',
                        interval=1000 * 60 * 60 * 24,
                        n_intervals=0
                    )
                ]
                )
            )
        ], style={'columnCount': 2}),
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.Graph(id='live-graph', animate=True),
                    dcc.Interval(
                        id='live-update',
                        interval=1000,
                        n_intervals=0
                    )
                ]
                )),
            dbc.Col(
                html.Div([
                    dcc.Graph(id='minute-graph', animate=True),
                    dcc.Interval(
                        id='minute-update',
                        interval=1000 * 60,
                        n_intervals=0
                    )
                ]
                )
            )
        ], style={'columnCount': 2})
    ]  # , style={'columnCount': 2}
    )
    return layout


def init_callbacks(dash_app, session, x_name, y_name,
                   # prepared_query_youtube_day, prepared_query_twitter_day, prepared_query_twitch_day,
                   # prepared_query_youtube_hour, prepared_query_twitter_hour, prepared_query_twitch_hour,
                   # prepared_query_youtube_minute, prepared_query_twitter_minute, prepared_query_twitch_minute,
                   prepared_query_day, prepared_query_hour, prepared_query_minute,
                   prepared_query_youtube_live, prepared_query_twitter_live, prepared_query_twitch_live):
    # @dash_app.callback(
    #     # ... Callback input/output
    #     )
    # def update_graph():
    #     pass
    #     # ... Insert callback stuff here
    @dash_app.callback(Output('live-graph', 'figure'),
                       [Input('live-update', 'n_intervals'),
                        Input('user', 'value')])
    def update_live_graph(n, user):

        session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
        session.default_fetch_size = 100000

        df_youtube = session.execute(prepared_query_youtube_live, (user,))._current_rows
        df_twitch = session.execute(prepared_query_twitch_live, (user,))._current_rows
        df_twitter = session.execute(prepared_query_twitter_live, (user,))._current_rows

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
                mode='lines',
                line=dict(color=colors[i], width=line_size[i]),
                marker=dict(color=colors[i], size=mode_size[i]),
                text=labels[i],
                name='',
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
                showgrid=True,
                zeroline=False,
                showline=True,
                showticklabels=True,
            ),
            autosize=False,
            margin=dict(
                autoexpand=False,
                l=100,
                r=20,
                t=110,
            ),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        # Title
        annotations = [dict(xref='paper', yref='paper', x=0.0, y=1.05,
                            xanchor='left', yanchor='bottom',
                            # text=user + ' Social Media Live',
                            text='Social Media Live',
                            font=dict(family='Arial',
                                      size=30,
                                      color='rgb(37,37,37)'),
                            showarrow=False)]

        layout['annotations'] = annotations
        return {'data': traces, 'layout': layout}

    @dash_app.callback(Output('minute-graph', 'figure'),
                       [Input('minute-update', 'n_intervals'),
                        Input('user', 'value')])
    def update_minute_graph(n, user):

        session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
        session.default_fetch_size = 100000

        # df_youtube = session.execute(prepared_query_youtube_minute, (user,))._current_rows
        # df_twitch = session.execute(prepared_query_twitch_minute, (user,))._current_rows
        # df_twitter = session.execute(prepared_query_twitter_minute, (user,))._current_rows

        df_minute = session.execute(prepared_query_minute, (user,))._current_rows

        title = user + 'Social Media'

        labels = ['YouTube', 'Twitter', 'Twitch']

        colors = ['rgb(255,0,0)', 'rgb(0,172,237)', 'rgb(100,65,165)']

        mode_size = [8, 8, 8]

        line_size = [2, 2, 2]

        x_data = [df_minute[x_name], df_minute[x_name], df_minute[x_name]]

        y_data = [df_minute['youtube_count'], df_minute['twitter_count'], df_minute['twitch_count']]

        traces = []

        for i in range(0, 3):
            traces.append(go.Scatter(
                x=x_data[i],
                y=y_data[i],
                mode='lines',
                line=dict(color=colors[i], width=line_size[i]),
                marker=dict(color=colors[i], size=mode_size[i]),
                text=labels[i],
                name='',
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
                showgrid=True,
                zeroline=False,
                showline=True,
                showticklabels=True,
            ),
            autosize=False,
            margin=dict(
                autoexpand=False,
                l=100,
                r=20,
                t=110,
            ),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        # Title
        annotations = [dict(xref='paper', yref='paper', x=0.0, y=1.05,
                            xanchor='left', yanchor='bottom',
                            # text=user + ' Social Media by Minute',
                            text='Social Media by Minute',
                            font=dict(family='Arial',
                                      size=30,
                                      color='rgb(37,37,37)'),
                            showarrow=False)]

        layout['annotations'] = annotations
        return {'data': traces, 'layout': layout}

    @dash_app.callback(Output('hour-graph', 'figure'),
                       [Input('hour-update', 'n_intervals'),
                        Input('user', 'value')])
    def update_hour_graph(n, user):

        session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
        session.default_fetch_size = 100000

        # df_youtube = session.execute(prepared_query_youtube_hour, (user,))._current_rows
        # df_twitch = session.execute(prepared_query_twitch_hour, (user,))._current_rows
        # df_twitter = session.execute(prepared_query_twitter_hour, (user,))._current_rows

        df_hour = session.execute(prepared_query_hour, (user,))._current_rows

        title = user + 'Social Media'

        labels = ['YouTube', 'Twitter', 'Twitch']

        colors = ['rgb(255,0,0)', 'rgb(0,172,237)', 'rgb(100,65,165)']

        mode_size = [8, 8, 8]

        line_size = [2, 2, 2]

        x_data = [df_hour[x_name], df_hour[x_name], df_hour[x_name]]

        y_data = [df_hour['youtube_count'], df_hour['twitter_count'], df_hour['twitch_count']]

        traces = []

        for i in range(0, 3):
            traces.append(go.Scatter(
                x=x_data[i],
                y=y_data[i],
                mode='lines',
                line=dict(color=colors[i], width=line_size[i]),
                marker=dict(color=colors[i], size=mode_size[i]),
                text=labels[i],
                name='',
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
                showgrid=True,
                zeroline=False,
                showline=True,
                showticklabels=True,
            ),
            autosize=False,
            margin=dict(
                autoexpand=False,
                l=100,
                r=20,
                t=110,
            ),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        # Title
        annotations = [dict(xref='paper', yref='paper', x=0.0, y=1.05,
                            xanchor='left', yanchor='bottom',
                            # text=user + ' Social Media by Hour',
                            text='Social Media by Hour',
                            font=dict(family='Arial',
                                      size=30,
                                      color='rgb(37,37,37)'),
                            showarrow=False)]

        layout['annotations'] = annotations
        return {'data': traces, 'layout': layout}

    @dash_app.callback(Output('day-graph', 'figure'),
                       [Input('day-update', 'n_intervals'),
                        Input('user', 'value')])
    def update_day_graph(n, user):

        session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
        session.default_fetch_size = 100000

        # df_youtube = session.execute(prepared_query_youtube_day, (user,))._current_rows
        # df_twitch = session.execute(prepared_query_twitch_day, (user,))._current_rows
        # df_twitter = session.execute(prepared_query_twitter_day, (user,))._current_rows

        df_day = session.execute(prepared_query_day, (user,))._current_rows

        title = user + 'Social Media'

        labels = ['YouTube', 'Twitter', 'Twitch']

        colors = ['rgb(255,0,0)', 'rgb(0,172,237)', 'rgb(100,65,165)']

        mode_size = [8, 8, 8]

        line_size = [2, 2, 2]

        x_data = [df_day[x_name], df_day[x_name], df_day[x_name]]

        y_data = [df_day['youtube_count'], df_day['twitter_count'], df_day['twitch_count']]

        traces = []

        for i in range(0, 3):
            traces.append(go.Scatter(
                x=x_data[i],
                y=y_data[i],
                mode='lines',
                line=dict(color=colors[i], width=line_size[i]),
                marker=dict(color=colors[i], size=mode_size[i]),
                text=labels[i],
                name='',
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
                showgrid=True,
                zeroline=False,
                showline=True,
                showticklabels=True,
            ),
            autosize=False,
            margin=dict(
                autoexpand=False,
                l=100,
                r=20,
                t=110,
            ),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        # Title
        annotations = [dict(xref='paper', yref='paper', x=0.0, y=1.05,
                            xanchor='left', yanchor='bottom',
                            # text=user + ' Social Media by Day',
                            text='Social Media by Day',
                            font=dict(family='Arial',
                                      size=30,
                                      color='rgb(37,37,37)'),
                            showarrow=False)]

        layout['annotations'] = annotations
        return {'data': traces, 'layout': layout}
