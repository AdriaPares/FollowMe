"""Create a Dash app within a Flask app."""

from dash import Dash
import pandas as pd
from .layout import html_layout
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
from cassandra.cluster import Cluster, Session
from cassandra.query import PreparedStatement, SimpleStatement
import datetime as dt
import dash_bootstrap_components as dbc
import dash_table


def connect_cassandra_cluster(cluster=('10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'), keyspace='insight'):
    cluster = Cluster(list(cluster))
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session


def get_data_frame_with_parameter(session: Session, prepared_query: PreparedStatement, parameter: str) -> pd.DataFrame:
    return session.execute(prepared_query, (parameter,))._current_rows


def get_data_frame_no_parameter(session: Session, prepared_query: str) -> pd.DataFrame:
    return session.execute(prepared_query)._current_rows


def get_joined_data_frame(session: Session, query_accounts: str, query_games: str,
                          prepared_query_day: PreparedStatement, timestamp: str) -> pd.DataFrame:
    # join users, accounts and games

    accounts_df = get_data_frame_no_parameter(session, query_accounts)
    games_df = get_data_frame_no_parameter(session, query_games)
    table_df = get_data_frame_with_parameter(session, prepared_query_day, timestamp)

    print(accounts_df)
    print(games_df)
    print(table_df)

    accounts_df\
        .join(games_df, accounts_df.game == games_df.game)\
        .join(table_df, accounts_df.streamer == table_df.streamer)

    return accounts_df


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
                    routes_pathname_prefix='/ranking/')

    # Override the underlying HTML template
    dash_app.index_string = html_layout

    # Add Cassandra Queries and Parameters
    # Pass this as a dictionary of parameters, hidden in a function

    session = connect_cassandra_cluster()
    session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
    session.default_fetch_size = 100000

    day = '2019-07-03'

    date_dict = {
        'yesterday': dt.datetime.strftime(dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(days=1), '%Y-%m-%d'),
        'last_week': dt.datetime.strftime(dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(weeks=1), '%Y-%m-%d'),
        'last_month': dt.datetime.strftime(dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(weeks=4), '%Y-%m-%d')
    }

    prepared_query_day = session.prepare("select streamer, twitch_count, twitter_count, youtube_count, total_count "
                                         "from unified_day_view where timestamp=?")
    query_accounts = 'select streamer, game, language from accounts'
    query_games = 'select game, console, genre from games'

    # Create Dash Layout comprised of Data Tables

    dash_app.layout = html.Div(
        children=get_categories(),
        id='dash-container'
      )

    init_callbacks(dash_app, date_dict, session, query_accounts, query_games, prepared_query_day, day)

    return dash_app.server


def get_categories():
    """Creates table with ranking by categories"""
    layout = html.Div([
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='date',
                        options=[
                            {'label': 'Yesterday', 'value': 'yesterday'},
                            {'label': 'Last Week', 'value': 'last_week'},
                            {'label': 'Last Month', 'value': 'last_month'}
                        ],
                        value='yesterday'
                    ),
                    dcc.Dropdown(
                        id='category',
                        options=[
                            {'label': 'Games', 'value': 'game'},
                            {'label': 'Genre', 'value': 'genre'},
                            {'label': 'Language', 'value': 'language'},
                            {'label': 'Console', 'value': 'console'},
                        ],
                        value='game'
                    )
                ])
            ),
        ]),
        dbc.Row(
            dbc.Col(
                html.Div([
                    dash_table.DataTable(
                        id='table'
                    )
                ])
            )
        )

    ])
    return layout


def init_callbacks(dash_app, date_dict, session, query_accounts, query_games, prepared_query_day, day):

    @dash_app.callback([Output('table', 'data'),
                        Output('table', 'columns')],
                       [Input('date', 'value'),
                        Input('category', 'value')])
    def update_table(date, category):
        query_timestamp = date_dict[date]
        # filter by timestamp
        table_df = get_joined_data_frame(session, query_accounts, query_games, prepared_query_day, day)
        print(table_df)
        columns = [{'id': c, 'name': c} for c in table_df.columns]
        table_data = table_df.sort_values(by=['total_count'], ascending=False).to_dict('records')

        return table_data, columns
