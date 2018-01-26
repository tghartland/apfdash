import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import numpy as np
import plotly


import plotly.plotly as py
import plotly.graph_objs as go

import pandas as pd

from app import app

from datasources import Datasources

# Display names for groupings
groupings = {
    "gridresource": "Grid resource",
    "globaljobid": "Factory",
    "jobstatus": "Job status",
    "pandacount": "Empty/Payload",
}

def generate_plot(grouping):
    if grouping is None:
        return {}
    dataframe = Datasources.get_latest_data_for("aws-athena-query-results-lancs-4h")
    if len(dataframe) == 0:
        return {}
    dataframe = dataframe.sample(frac=0.25, random_state=0)
    dataframe["gridresource"] = dataframe["gridresource"].apply(lambda x: x.split()[0])
    dataframe["globaljobid"] = dataframe["globaljobid"].apply(lambda x: x.split(".")[0])

    traces = []
    
    if grouping == "pandacount":
        dataframe["pandacount"] = dataframe["pandacount"].apply(lambda x: int(x>0))
    
    for group in dataframe[grouping].unique():
        filtered_df = dataframe[dataframe[grouping] == group]
        if grouping == "pandacount":
            group = {0:"Empty", 1:"Payload"}.get(group)
        traces.append(go.Scatter(
            x=filtered_df["remotewallclocktime"],
            y=filtered_df["remoteusercpu"]+filtered_df["remotesyscpu"],
            mode="markers",
            opacity=0.7,
            name=group,
            marker={
                "size":5,
            },
            hoverinfo="none",
        ))

    layout = go.Layout(
        xaxis={"title": "Remote wallclock time"},
        yaxis={"title": "Remote user+sys cpu time"},
        title="Events in past 4 hours ({})".format(groupings[grouping]),
        margin={
            "r":220,
            "l":60,
            "t":50,
            "b":50,
        },
        width=830,
        height=630,
    )

    figure = {
        "data": traces,
        "layout": layout,
    }

    return figure


@app.callback(
    Output("grouping-plot", "figure"),
    [Input("grouping-dropdown", "value")],
)
def update_plot(grouping):
    return generate_plot(grouping)
    
help_panel = [
    html.P("Click the dropdown for other grouping options."),
]

def generate_layout():
    if len(Datasources.get_latest_data_for("aws-athena-query-results-lancs-4h")) == 0:
        return "No jobs in past 4 hours"
    layout = [
        html.Div([
            html.Div([
                dcc.Dropdown(
                    options=[{"label":name, "value":group} for group, name in groupings.items()],
                    value="gridresource",
                    clearable=False,
                    id="grouping-dropdown",
                ),
            ],
            style={
                "width":"250px",
            }),
            
            html.Div([
                dcc.Graph(
                    figure=generate_plot(None),
                    config={'displayModeBar': False},
                    id="grouping-plot",)
            ],
            style={
                "width": "50%",
                "float": "left",
            }),

            html.Div("Actually based on 25% sample of data"),
        ],
        style={
            "height": "75%",
        })
    ]

    return help_panel, layout
