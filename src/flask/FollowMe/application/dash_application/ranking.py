import datetime as dt
import dash_table
from .flask_functions import *


def get_joined_data_frame(session: Session, query_accounts: str, query_games: str,
                          prepared_query_day: PreparedStatement, timestamp: str) -> pd.DataFrame:

    """Join users, accounts and games tables from Cassandra"""

    accounts_df = get_data_frame_no_parameter(session, query_accounts)
    games_df = get_data_frame_no_parameter(session, query_games)
    table_df = get_data_frame_with_parameter(session, prepared_query_day, timestamp)

    return accounts_df.merge(games_df, on='game', how='inner').merge(table_df, on='streamer', how='inner')


def get_options(session: Session, prepared_query: str, column: str) -> list:
    """Gets options for DropDown"""

    option_df = get_data_frame_no_parameter(session, prepared_query)
    return list(option_df[column].unique())


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

    # Connect to Cassandra
    session = connect_cassandra_cluster()

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

    all_options = {
        'game': get_options(session, query_games, 'game'),
        'genre': get_options(session, query_games, 'genre'),
        'console': get_options(session, query_games, 'console'),
        'language': get_options(session, query_accounts, 'language')
    }

    # Create Dash Layout comprised of Data Tables

    dash_app.layout = html.Div(
        children=get_categories(),
        id='dash-container'
      )

    init_callbacks(dash_app, date_dict, session, query_accounts, query_games, prepared_query_day, all_options)

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
                    )
                ]),
            )
        ]),
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='category',
                        options=[
                            {'label': 'Games', 'value': 'game'},
                            {'label': 'Genre', 'value': 'genre'},
                            {'label': 'Language', 'value': 'language'},
                            {'label': 'Console', 'value': 'console'},
                        ],
                        value='game'
                    ),
                ], style={'width': '49%'}),
            ),
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='subcategory'
                    )
                ], style={'width': '49%'})
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


def init_callbacks(dash_app, date_dict, session, query_accounts, query_games, prepared_query_day, all_options):

    @dash_app.callback([Output('subcategory', 'options'),
                        Output('subcategory', 'value')],
                       [Input('category', 'value')])
    def update_subcategory(category):
        return [{'label': i, 'value': i} for i in all_options[category]], all_options[category][0]

    @dash_app.callback([Output('table', 'data'),
                        Output('table', 'columns')],
                       [Input('date', 'value'),
                        Input('category', 'value'),
                        Input('subcategory', 'value')])
    def update_table(date, category, subcategory):

        query_timestamp = date_dict[date]

        # filter by timestamp
        table_df = get_joined_data_frame(session, query_accounts, query_games, prepared_query_day, query_timestamp)
        columns_to_show = ['streamer', category, 'youtube_count', 'twitch_count', 'twitter_count', 'total_count']
        columns = [{'id': c, 'name': c} for c in columns_to_show]
        table_data = table_df[columns_to_show][table_df[category] == subcategory]\
            .sort_values(by=['total_count'], ascending=False)\
            .to_dict('records')

        return table_data, columns
