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
            showlegend=True,
            legend=dict(x=0, y=.8),
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


def connect_cassandra_cluster(cluster=('10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'), keyspace='insight'):
    cluster = Cluster(list(cluster))
    session = cluster.connect()
    session.set_keyspace(keyspace)
    session.row_factory = lambda x, y: pd.DataFrame(y, columns=x)
    session.default_fetch_size = 100000
    return session


def get_data_frame_with_parameter(session: Session, prepared_query: PreparedStatement, parameter: str) -> pd.DataFrame:
    return session.execute(prepared_query, (parameter,))._current_rows


def get_data_frame_no_parameter(session: Session, prepared_query: str) -> pd.DataFrame:
    return session.execute(prepared_query)._current_rows