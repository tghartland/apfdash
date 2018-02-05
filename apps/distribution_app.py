from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

from datasources import Datasources


def generate_binned_plot_1s():
    dataframe = Datasources.get_latest_data_for("aws-athena-apfdash-dist-binned1s")
    # dataframe["total_jobs"]-dataframe["empty_jobs"]

    empty_hist = go.Bar(x=dataframe["remotewallclocktime"], y=dataframe["empty_jobs"], name="Empty", marker={"color":"#C21E29"}, width=1)
    payload_hist = go.Bar(x=dataframe["remotewallclocktime"], y=dataframe["total_jobs"]-dataframe["empty_jobs"], name="Payload", marker={"color":"#3A6CAC"}, width=1)

    data = [empty_hist, payload_hist]

    layout = go.Layout(
        title="Wallclock time (<1200s) distribution of jobs in past 48 hours",
        barmode="stack",
        xaxis=go.XAxis(
            title="remotewallclocktime (seconds)",
            showgrid=False,
        ),
        yaxis=go.YAxis(
            title="Jobs",
            showgrid=True,
            #autorange=False,
            #range=[0, 100],
            type="log",
        )
    )

    figure = {
        "data": data,
        "layout": layout,
    }

    return dcc.Graph(id="distribution-plot-seconds", figure=figure, config={"displayModeBar": False})

def generate_binned_plot_10m():
    dataframe = Datasources.get_latest_data_for("aws-athena-apfdash-dist-binned1m")
    # dataframe["total_jobs"]-dataframe["empty_jobs"]

    empty_hist = go.Bar(x=dataframe["minutes"], y=dataframe["empty_jobs"], name="Empty", marker={"color":"#C21E29"}, width=1)
    payload_hist = go.Bar(x=dataframe["minutes"], y=dataframe["total_jobs"]-dataframe["empty_jobs"], name="Payload", marker={"color":"#3A6CAC"}, width=1)

    data = [empty_hist, payload_hist]

    layout = go.Layout(
        title="Wallclock time binned by minutes of all jobs in past 48 hours",
        barmode="stack",
        xaxis=go.XAxis(
            title="Minutes",
            showgrid=False,
        ),
        yaxis=go.YAxis(
            title="Jobs",
            showgrid=True,
            #autorange=False,
            #range=[0, 100],
            type="log",
        )
    )

    figure = {
        "data": data,
        "layout": layout,
    }

    return dcc.Graph(id="distribution-plot-minutes", figure=figure, config={"displayModeBar": False})

help_panel = [
    html.P("10k minutes is 7 days."),
]

def generate_layout():
    if len(Datasources.get_latest_data_for("aws-athena-apfdash-dist-binned1s")) == 0:
        return "No jobs in past 48 hours"
    layout = [
        html.Div([
            #generate_plot("duration"),
            #generate_plot("remotewallclocktime"),
            generate_binned_plot_1s(),
            generate_binned_plot_10m(),
        ],
        style={
        })
    ]

    return help_panel, layout
