from datetime import datetime, timedelta, date

from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import plotly.plotly as py
import plotly.graph_objs as go

import pandas as pd

from datasources import Datasources

def generate_plot_24h(queue_name):
    df = Datasources.get_latest_data_for("aws-athena-query-results-lancs-24h")
    filtered_df = df[df["match_apf_queue"] == queue_name]
    
    filtered_df.insert(4, "long_jobs", filtered_df["total_jobs"]-filtered_df["empty_jobs"])
    
    trace1 = go.Bar(
        x=filtered_df["job_time"],
        y=filtered_df["empty_jobs"],
        name="Empty",
        marker=dict(
            color="#C21E29",
        ),
        offset=0.5,
    )
    
    trace2 = go.Bar(
        x=filtered_df["job_time"],
        y=filtered_df["long_jobs"],
        name="Payload",
        marker=dict(
            color="#3A6CAC",
        ),
        offset=0.5,
    )
    
    data = [trace1, trace2]
    
    min_time = df["job_time"].min()
    max_time = df["job_time"].max()
    
    layout = go.Layout(
        title="24 hours",
        barmode="stack",
        xaxis = go.XAxis(
            title="Time",
            autorange=False,
            range=[min_time, max_time],
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
        yaxis = go.YAxis(
            title="Jobs",
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
        margin=go.Margin(
            l=55,
            r=45,
            b=50,
            t=30,
            pad=4
        ),
    )
    
    fig = {
        "data": data,
        "layout": layout,
    }
    
    return dcc.Graph(id="queue-24h", figure=fig, config={'displayModeBar': False})


def generate_plot_30d(queue_name):
    df = Datasources.get_latest_data_for("aws-athena-query-results-lancs-history-30d")
    df["job_date"] = df["job_date"].apply(lambda d: datetime.date(datetime.strptime(d, "%Y-%m-%d")))
    filtered_df = df[df["match_apf_queue"] == queue_name]
    
    filtered_df.insert(4, "long_jobs", filtered_df["total_jobs"]-filtered_df["empty_jobs"])
    
    #filtered_df.loc[-1] = [queue_name, datetime.today()-timedelta(60), 20, 10, 10]
    
    max_date = date.today()+timedelta(days=1)
    # One day is added to the max date as the bars are positioned so
    # that their left edge is on the day they represent.
    # Without the +1 day the most recent bar is excluded.
    min_date = date.today()-timedelta(days=30)
    
    # Add some data outside the range that will be displayed in the chart.
    # This fixes the issue where if there was only one entry in the dataframe,
    # the bar chart would not figure out that the bars were supposed
    # to be one day wide.
    for _date in [min_date-timedelta(days=60+i) for i in range(0, 10)]:
        tmp_df = pd.DataFrame({
            "match_apf_queue":queue_name,
            "job_date":_date,
            "total_jobs":0,
            "empty_jobs":0,
            "long_jobs":0
        }, index=[0])
        filtered_df = filtered_df.append(tmp_df, ignore_index=True)
    
    
    trace1 = go.Bar(
        x=filtered_df["job_date"],
        y=filtered_df["empty_jobs"],
        name="Empty",
        marker=dict(
            color="#C21E29",
        ),
        offset=0.5,
    )
    
    trace2 = go.Bar(
        x=filtered_df["job_date"],
        y=filtered_df["long_jobs"],
        name="Payload",
        marker=dict(
            color="#3A6CAC",
        ),
        #width=24*3600*1000,
        offset=0.5,
    )
    
    data = [trace1, trace2]
    
    
    layout = go.Layout(
        title="30 days",
        barmode="stack",
        xaxis = go.XAxis(
            title="Date",
            autorange=False,
            range=[min_date, max_date],
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
        yaxis = go.YAxis(
            title="Jobs",
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
        margin=go.Margin(
            l=55,
            r=45,
            b=50,
            t=30,
            pad=4
        ),
    )
    
    fig = {
        "data": data,
        "layout": layout,
    }
    
    return dcc.Graph(id="queue-30d", figure=fig, config={'displayModeBar': False})


def generate_distribution(queue_name, ten_minutes=False):
    dataframe = Datasources.get_latest_data_for("aws-athena-query-results-lancs-4h")
    dataframe = dataframe[dataframe["match_apf_queue"] == queue_name]
    
    if len(dataframe) == 0:
        max_duration = 600
    else:
        max_duration = max(dataframe["remotewallclocktime"])
    
    x_range = [0, max_duration]
    
    if ten_minutes:
        dataframe = dataframe[dataframe["remotewallclocktime"] <= 600]
        x_range = [0, 600]
    
    empty = dataframe[dataframe["pandacount"]==0]
    nonempty = dataframe[dataframe["pandacount"]>0]
    
    if ten_minutes:
        hist1 = go.Histogram(x=empty["remotewallclocktime"], xbins={"start":0, "end":600, "size":12}, marker={"color":"#C21E29"}, name="Empty")
        hist2 = go.Histogram(x=nonempty["remotewallclocktime"], xbins={"start":0, "end":600, "size":12}, marker={"color":"#3A6CAC"}, name="Payload")
    else:
        # bins of minimum width 5 seconds
        bins = int(max_duration/5)
        # max 100 bins
        bins = min(bins, 100)
        hist1 = go.Histogram(x=empty["remotewallclocktime"], nbinsx=bins, marker={"color":"#C21E29"}, name="Empty")
        hist2 = go.Histogram(x=nonempty["remotewallclocktime"], nbinsx=bins, marker={"color":"#3A6CAC"}, name="Payload")
    
    layout = go.Layout(
        barmode="stack",
        title="remotewallclocktime distribution in past 4 hours{}".format(" (duration < 10m)" if ten_minutes else ""),
        xaxis = go.XAxis(
            title="remotewallclocktime (s)",
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
            autorange=False,
            range=x_range,
        ),
        yaxis = go.YAxis(
            title="Number of jobs",
            fixedrange=True,
            showgrid=True,
            gridcolor="darkgrey",
        ),
        margin=go.Margin(
            l=55,
            r=45,
            b=50,
            t=30,
            pad=4
        ),
    )
    
    fig = {
        "data": [hist1, hist2],
        "layout": layout,
    }
    
    
    return dcc.Graph(id="distribution-histogram-{}".format(["1", "2"][ten_minutes]), figure=fig, config={'displayModeBar': False})


def generate_layout(queue_name):
    plot = generate_plot_24h(queue_name)
    if plot is None:
        plotdiv = []
    else:
        plotdiv = [plot,]
        #return [html.Div("No queue with that name"),]
    
    plot2 = generate_plot_30d(queue_name)
    
    layout =  [
        html.H4(queue_name),
        html.Div([
            html.Div(
                plotdiv,
                style={
                    "width":"50%",
                    "display":"inline-block",
                }
            ),
            
            html.Div(
                [plot2,],
                style={
                    "width":"50%",
                    "display":"inline-block",
                }
            ),
        ],
        style={
        
        }),
        
        html.Div([
            html.Div([
                generate_distribution(queue_name),
            ],
            style={
                "width":"50%",
                "display":"inline-block",
            }),

            html.Div([
                generate_distribution(queue_name, ten_minutes=True),
            ],
            style={
                "width":"50%",
                "display":"inline-block",
            }),
            
        ],)
        
    ]
    return layout
