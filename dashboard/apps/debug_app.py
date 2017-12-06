import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import humanize
from datetime import datetime
from dateutil.tz import tzutc

from app import app

from datasources import Datasources

def debug_table():
    column_names = ["Bucket", "Latest object", "Object Modified", "Downloaded"]

    rows = [html.Tr([html.Td(n) for n in column_names])]
    
    for bucket_name, obj in Datasources.query_data.items():
        rows.append(html.Tr([
            html.Td(bucket_name),
            html.Td(obj["filename"]),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["modified"])),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["downloaded"])),
        ]))

    return html.Table(rows)

def generate_layout():
    layout = [
        dcc.Link("Index", href="/"),
        debug_table(),
    ]
    return layout
