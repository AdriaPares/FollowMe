#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from app import app

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/mtcars.csv')

if 'DYNO' in os.environ:
    app_name = os.environ['DASH_APP_NAME']
else:
    app_name = 'dash-multiplesubplot'

layout = html.Div([
    html.Div([html.H1("Car Performance Metrics")], style={"textAlign": "center"}),
    html.Div([html.Div([html.Div([html.Span("Scatter Plot x-axis", className="three columns",
                                            style={"width": 150, "padding": 10, "text-align": "right"}),
                                  html.Div(dcc.Dropdown(id="xaxis",
                                                        options=[{'label': "Miles per gallon", 'value': "mpg"},
                                                                 {'label': "Displacement", 'value': "disp"},
                                                                 {'label': "Rear axle ratio", 'value': "drat"},
                                                                 {'label': "Weight (1000 lbs)", 'value': "wt"},
                                                                 {'label': "1/4 mile time", 'value': "qsec"},
                                                                 {'label': "Gross horsepower", 'value': "hp"}],
                                                        value='disp', ), className="three columns",
                                           style={"width": 250, "margin": 0})], className="row"),
                        html.Div([html.Span("Scatter Plot y-axis", className="three columns",
                                            style={"width": 150, "padding": 10, "text-align": "right"}),
                                  html.Div(dcc.Dropdown(id="yaxis",
                                                        options=[{'label': "Miles per gallon", 'value': "mpg"},
                                                                 {'label': "Displacement", 'value': "disp"},
                                                                 {'label': "Rear axle ratio", 'value': "drat"},
                                                                 {'label': "Weight (1000 lbs)", 'value': "wt"},
                                                                 {'label': "1/4 mile time", 'value': "qsec"},
                                                                 {'label': "Gross horsepower", 'value': "hp"}],
                                                        value='mpg', ), className="three columns",
                                           style={"width": 250, "margin": 0})], className="row"),
                        html.Div([html.Span("Bar Plot y-axis", className="three columns",
                                            style={"width": 150, "padding": 10, "text-align": "right"}),
                                  html.Div(dcc.Dropdown(id="barplot-yaxis",
                                                        options=[{'label': "Miles per gallon ", 'value': "mpg"},
                                                                 {'label': "Displacement", 'value': "disp"},
                                                                 {'label': "Rear axle ratio", 'value': "drat"},
                                                                 {'label': "Weight (1000 lbs)", 'value': "wt"},
                                                                 {'label': "1/4 mile time ", 'value': "qsec"},
                                                                 {'label': "Gross horsepower", 'value': "hp"}],
                                                        value='hp', ), className="three columns",
                                           style={"width": 250, "margin": 0})], className="row"), ],
                       style={'width': '48%', 'display': 'inline-block'}, className="six columns"),
              html.Div([html.Div([html.Span("Box Plot x-axis", className="three columns",
                                            style={"text-align": "right", "width": "30%", "padding": 5}),
                                  html.Div(dcc.RadioItems(id="select-value", value='vs',
                                                          options=[{'label': "Transmission", 'value': "am"},
                                                                   {'label': "Engine-Type", 'value': "vs"}],
                                                          labelStyle={'display': 'inline', "padding": 7.5}),
                                           className="three columns",
                                           style={"width": "50%", "margin": 0, "padding": 5})], className="row"),
                        html.Div([html.Span("Box Plot y-axis", className="three columns",
                                            style={"width": 150, "padding": 10, "text-align": "right"}),
                                  html.Div(dcc.Dropdown(id="boxplot-yaxis",
                                                        options=[{'label': "Miles per gallon ", 'value': "mpg"},
                                                                 {'label': "Displacement", 'value': "disp"},
                                                                 {'label': "Rear axle ratio", 'value': "drat"},
                                                                 {'label': "Weight (1000 lbs)", 'value': "wt"},
                                                                 {'label': "1/4 mile time ", 'value': "qsec"},
                                                                 {'label': "Gross horsepower", 'value': "hp"}],
                                                        value='mpg', ), className="three columns",
                                           style={"width": 250, "margin": 0})], className="row")
                        ], style={'width': '50%', 'float': 'right', 'display': 'inline-block', "margin-left": "auto",
                                  "margin-right": "auto", "padding-top": 30}, className="six columns")],
             className="row"),
    html.Div([dcc.Graph(id="my-graph")]),
], className="container")


@app.callback(
    dash.dependencies.Output("my-graph", "figure"),
    [dash.dependencies.Input("xaxis", "value"), dash.dependencies.Input("yaxis", "value"),
     dash.dependencies.Input("boxplot-yaxis", "value"), dash.dependencies.Input("select-value", "value"),
     dash.dependencies.Input("barplot-yaxis", "value")])
def update_graph(selected1, selected2, selected_box_y, selected_box_x, selected_bar_y):
    trace1 = [go.Scatter(x=df[selected1], y=df[selected2], text=df['manufacturer'], mode='markers', opacity=0.8,
                         marker={'size': 10, "color": "#00CC94", }, showlegend=False, )]

    trace2 = [go.Box(x=df[selected_box_x], y=df[selected_box_y],
                     name=f'{"Transmission" if selected_box_x == "am" else "Engine Type"}', showlegend=False,
                     xaxis='x2', yaxis='y2', marker={"color": "#4CEB00"})]
    trace3 = [go.Bar(x=df['manufacturer'], y=df[selected_bar_y], xaxis='x3', yaxis='y3', showlegend=False,
                     marker={"color": "#5DB4F2", "opacity": 0.9})]

    trace = trace1 + trace2 + trace3
    text = {"mpg": "Miles per gallon (mpg)", "disp": "Displacement (cu.in.)", "drat": "Rear axle ratio (drat)",
            "qsec": "1/4 mile time (qsec)", "wt": "Weight (1000 lbs)", "hp": "Gross horsepower (hp)"}
    return {"data": trace,
            "layout": go.Layout(height=850, width=1000, annotations=[
                {"x": 0.07, "y": 1, "xref": "paper", "yref": "paper","font": {"size": 12},
                 "text": f'{text[selected2].title()} vs {text[selected1].title()}', "showarrow": False},
                {"x": 0.85, "y": 1, "xref": "paper", "yref": "paper","showarrow": False, "font": {"size": 12},
                 "text": f'{text[selected_box_y].title()} vs 'f'{"Transmission" if selected_box_x == "am" else "Engine Type"}'},
                {"x": 0.5, "y": 0.45, "xref": "paper", "yref": "paper",
                 "text": f'{text[selected_bar_y].title()} Vs Car Model', "showarrow": False, "font": {"size": 15}}],
                                xaxis={"title": text[selected1].title(), "domain": [0, 0.40], "anchor": 'y', },
                                yaxis={"title": text[selected2].title(), "domain": [0.65, 1], "anchor": 'x'}, xaxis2={
                    "title": f'{"Automatic Transmission   Manual Transmission " if selected_box_x == "am" else "V-shaped Engine   Straight Engine"}',
                    "domain": [0.55, 1], "anchor": 'y2',
                    "showticklabels": False, },
                                yaxis2={"title": text[selected_box_y].title(), "domain": [0.55, 1], "anchor": 'x2'},
                                xaxis3={"tickangle": -38, "domain": [0, 1], "anchor": 'y3'},
                                yaxis3={"title": text[selected_bar_y].title(), "domain": [0, 0.45], "anchor": 'x3'})
            }