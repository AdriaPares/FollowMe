"""Create a Dash app within a Flask app."""

import datetime as dt
import dash_table
from .flask_functions import *


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
                    routes_pathname_prefix='/categories/')

    # Override the underlying HTML template
    dash_app.index_string = html_layout

    # Initialize Cassandra Session
    session = connect_cassandra_cluster()

    # Set the day and date dictionary
    day = '2019-05-26'
    date_dict = {
        'yesterday': dt.datetime.strftime(dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(days=1), '%Y-%m-%d'),
        'last_week': dt.datetime.strftime(dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(weeks=1), '%Y-%m-%d'),
        'last_month': dt.datetime.strftime(dt.datetime.strptime(day, '%Y-%m-%d') - dt.timedelta(weeks=4), '%Y-%m-%d')
    }

    # Create Dash Layout comprised of Data Tables
    dash_app.layout = html.Div(
        children=get_categories(),
        id='dash-container'
      )

    init_callbacks(dash_app, session, date_dict)

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


def init_callbacks(dash_app, session, date_dict):

    @dash_app.callback([Output('table', 'data'),
                        Output('table', 'columns')],
                       [Input('date', 'value'),
                        Input('category', 'value')])
    def update_table(date, category):

        query_timestamp = date_dict[date]
        table_df = session.execute("select " + category + ", total_count from " + category +
                                   "_aggregation where timestamp='" + query_timestamp +"' ;")._current_rows
        columns = [{'id': c, 'name': c} for c in table_df.columns]
        table_data = table_df.sort_values(by=['total_count'], ascending=False).to_dict('records')

        return table_data, columns
