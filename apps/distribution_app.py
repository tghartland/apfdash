from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

from app import app

from datasources import Datasources


def generate_plot(column):
    duration_limit = 1200
    dataframe = Datasources.get_latest_data_for("aws-athena-query-results-lancs-all-48h")
    dataframe = dataframe[dataframe[column] <= duration_limit]
    
    empty = dataframe[dataframe["pandacount"]==0]
    nonempty = dataframe[dataframe["pandacount"]>0]
    
    
    empty_hist = go.Histogram(x=empty[column], xbins={"start":-0.5, "end":duration_limit-0.5, "size":1}, marker={"color":"#C21E29"}, name="Empty")
    nonempty_hist = go.Histogram(x=nonempty[column], xbins={"start":-0.5, "end":duration_limit-0.5, "size":1}, marker={"color":"#3A6CAC"}, name="Payload")

    layout = go.Layout(
        barmode="stack",
        xaxis={"title": column},
        yaxis={"title": "Num jobs", "type":"log"},
        title="{} distribution of all jobs in past 48 hours".format(column),
        margin={
            "t":50,
        }
    )

    figure = {
        "data": [empty_hist, nonempty_hist],
        "layout": layout,
    }

    return dcc.Graph(id="distribution-plot-{}".format(column), figure=figure, config={'displayModeBar': False})


def generate_layout():
    if len(Datasources.get_latest_data_for("aws-athena-query-results-lancs-all-48h")) == 0:
        return "No jobs in past 48 hours"
    layout = [
        html.Div([
            generate_plot("duration"),
            generate_plot("remotewallclocktime"),
        ],
        style={
        })
    ]

    return layout
