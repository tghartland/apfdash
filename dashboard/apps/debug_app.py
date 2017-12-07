import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import humanize
from datetime import datetime
from dateutil.tz import tzutc

from app import app

from datasources import Datasources, QueryHistory


def debug_table():
    column_names = ["Bucket", "Latest object", "Object modified", "Downloaded", "Checked for update"]

    rows = [html.Tr([html.Td(n) for n in column_names])]
    
    for bucket_name, obj in Datasources.query_data.items():
        rows.append(html.Tr([
            html.Td(bucket_name),
            html.Td(obj["filename"]),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["modified"])),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["downloaded"])),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["checked_for_update"])),
        ]))

    return html.Table(rows)


def debug_query_history():
    column_names = ["Query name", "Result file",
                    "Execution time", "Status", "Bytes scanned", "Cost", "Execution duration (ms)"]
    
    def format_row(row):
        yield row[0]
        yield row[2]
        yield row[3].strftime("%a %d %b %H:%M:%S")
        yield row[4]
        yield humanize.naturalsize(row[5])
        yield "${:.02f}".format(5*int(row[5])/1e12)
        yield humanize.intcomma(row[6])
    
    rows = [html.Tr([html.Td(n) for n in column_names])]
    
    for event in list(reversed(QueryHistory.history))[0:30]:
        rows.append(html.Tr(
            [html.Td(x) for x in format_row(event)]
            )
        )
    
    return html.Table(rows)

def generate_layout():
    layout = [
        html.Div([
            html.H4("Current data"),
            debug_table(),
        ], style={"margin-bottom":"100px"}),
        html.Div([
            html.H4("Query history"),
            debug_query_history(),
        ]),
    ]
    return layout
