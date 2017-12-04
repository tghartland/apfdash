from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

from werkzeug.routing import Map, Rule
from werkzeug.routing import NotFound, RequestRedirect

routing_map = Map([
    Rule("/", endpoint="index"),
    Rule("/queue/<string:queue_name>/", endpoint="queue"),
])

routes = routing_map.bind("")

from app import app
from apps import index_app
from apps import queue_app

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    print("routing pathname", pathname)
    while True:
        try:
            page, args = routes.match(pathname)
        except RequestRedirect as redirect:
            pathname = redirect.new_url.replace("http://", "")
        except NotFound:
            return "404"
        else:
            break
    
    if page == "index":
        return index_app.layout
    elif page == "queue":
        return queue_app.generate_layout(args.get("queue_name"))

if __name__ == '__main__':
    app.run_server(debug=True)