"""Create a Dash app within a Flask app."""
from dash import Dash
import pandas as pd
from .layout import html_layout
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from cassandra.cluster import Cluster, Session
from cassandra.query import PreparedStatement
import dash_bootstrap_components as dbc


def get_layout() -> go.Layout:
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
    return layout


def get_annotations(platform: str) -> list:
    return [dict(
            xref='paper', yref='paper', x=0.0, y=1.05,
            xanchor='left', yanchor='bottom',
            text=platform + ' Daily',
            font=dict(family='Arial', size=30, color='rgb(37,37,37)'),
            showarrow=False)]


def get_traces(x_data: list, y_data: list, colors: list, line_size: list,
               mode_size: list, labels: list, people: int) -> list:
    traces = []
    for i in range(people):
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
    return traces


def get_data_frame(session: Session, prepared_query: PreparedStatement, user: str) -> pd.DataFrame:
    return session.execute(prepared_query, (user,))._current_rows


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
                    routes_pathname_prefix='/comparison/')

    # Override the underlying HTML template
    dash_app.index_string = html_layout

    # Add Cassandra Queries and Parameters
    # Pass this as a dictionary of parameters, hidden in a function

    x_name = 'timestamp'
    cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    session = cluster.connect()
    session.set_keyspace('insight')

    prepared_query_day = session.prepare("select timestamp, twitch_count, twitter_count, youtube_count, total_count "
                                         "from unified_day where streamer=?")

    prepared_query_accounts = session.prepare("select streamer from insight.accounts;")

    # Get streamers names, and prepare the dropdown options
    session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
    session.default_fetch_size = 100000

    streamers = session.execute(prepared_query_accounts)._current_rows

    dropdown_options = [{'label': streamer, 'value': streamer} for streamer in streamers.streamer]

    # Create Dash Layout comprised of Graphs
    dash_app.layout = html.Div(
        children=get_followers(dropdown_options),
        id='dash-container'
    )

    # Initialize callbacks after our app is loaded
    # Pass dash_app as a parameter

    init_callbacks(dash_app, session, x_name, prepared_query_day)

    return dash_app.server


def get_followers(dropdown_options):
    """Plots 4 charts with live, minute, hour and daily follower counts"""
    layout = html.Div([
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='user_1',
                        options=dropdown_options,
                        value='ninja'
                    )
                ]
                )
            ),
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='user_2',
                        options=dropdown_options,
                        value='lec'
                    )
                ]
                )
            )
        ]),
        dbc.Row(
            dbc.Col(
                html.Div([
                    dcc.Graph(id='youtube-graph', animate=True)
                ]
                )
            ),
        ),
        dbc.Row(
            dbc.Col(
                html.Div([
                    dcc.Graph(id='twitter-graph', animate=True)
                ]
                )
            )
        ),
        dbc.Row(
            dbc.Col(
                html.Div([
                    dcc.Graph(id='twitch-graph', animate=True)
                ]
                )
            )
        )
    ]
    )
    return layout


def init_callbacks(dash_app, session, x_name, prepared_query_day):
    @dash_app.callback([Output('youtube-graph', 'figure'),
                        Output('twitter-graph', 'figure'),
                        Output('twitch-graph', 'figure')],
                       [Input('user_1', 'value'),
                        Input('user_2', 'value')])
    def update_youtube_graph(user_1, user_2):

        session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
        session.default_fetch_size = 100000

        df_user_1 = get_data_frame(session, prepared_query_day, user_1)
        df_user_2 = get_data_frame(session, prepared_query_day, user_2)

        labels = [user_1, user_2]

        colors_youtube = ['rgb(255,0,0)', 'rgb(255,0,0)']
        colors_twitter = ['rgb(0,172,237)', 'rgb(0,172,237)']
        colors_twitch = ['rgb(100,65,165)', 'rgb(100,65,165)']

        mode_size = [8, 8]

        line_size = [2, 2]

        x_data = [df_user_1[x_name], df_user_2[x_name]]

        y_data_youtube = [df_user_1['youtube_count'], df_user_2['youtube_count']]
        y_data_twitter = [df_user_1['twitter_count'], df_user_2['twitter_count']]
        y_data_twitch = [df_user_1['twitch_count'], df_user_2['twitch_count']]

        # Get Traces

        traces_youtube = get_traces(x_data, y_data_youtube, colors_youtube, line_size, mode_size, labels, 2)
        traces_twitter = get_traces(x_data, y_data_twitter, colors_twitter, line_size, mode_size, labels, 2)
        traces_twitch = get_traces(x_data, y_data_twitch, colors_twitch, line_size, mode_size, labels, 2)

        # Get Layouts

        layout_youtube = get_layout()
        layout_twitter = get_layout()
        layout_twitch = get_layout()

        # Get annotations

        layout_youtube['annotations'] = get_annotations('YouTube')
        layout_twitter['annotations'] = get_annotations('Twitter')
        layout_twitch['annotations'] = get_annotations('Twitch')

        return {'data': traces_youtube, 'layout': layout_youtube}, \
               {'data': traces_twitter, 'layout': layout_twitter},\
               {'data': traces_twitch, 'layout': layout_twitch}
