from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import plotly.plotly as py
import plotly.graph_objs as go

import pandas as pd

from app import app

df = pd.read_csv("https://s3.amazonaws.com/aws-athena-query-results-lancs/72829fca-edf6-4b0e-a936-72b65d9b5b7b.csv")
df["job_time"] = pd.to_datetime(df["job_time"], format="%Y-%m-%d %H:%M:%S.%f")

def generate_plot(queue_name):
    filtered_df = df[df["match_apf_queue"] == queue_name]
    
    if len(filtered_df) == 0:
        return None
    
    filtered_df.insert(4, "long_jobs", filtered_df["total_jobs"]-filtered_df["short_jobs"])
    
    trace1 = go.Bar(
        x=filtered_df["job_time"],
        y=filtered_df["short_jobs"],
        name="Short jobs",
        marker=dict(
            color="#C21E29",
        ),
        offset=0.5,
    )
    
    trace2 = go.Bar(
        x=filtered_df["job_time"],
        y=filtered_df["long_jobs"],
        name="Long jobs",
        marker=dict(
            color="#3A6CAC",
        ),
        offset=0.5,
    )
    
    data = [trace1, trace2]
    
    min_time = df["job_time"].min()
    max_time = df["job_time"].max()
    
    layout = go.Layout(
        title="{} over past 24 hours".format(queue_name),
        barmode="stack",
        xaxis = go.XAxis(
            autorange=False,
            range=[min_time, max_time],
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
        yaxis = go.YAxis(
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
    )
    
    fig = {
        "data": data,
        "layout": layout,
    }
    
    return dcc.Graph(id="queue", figure=fig, config={'displayModeBar': False})


def generate_layout(queue_name):
    plot = generate_plot(queue_name)
    if plot is None:
        return [html.Link("Index", href="/"), html.Div("No queue with that name"),]
    
    layout =  [
        dcc.Link("Index", href="/"),
        html.H4(queue_name),
        html.Div([
            plot,
        ],
        style=dict(
            width="60%",
        ))
        
    ]
    return layout