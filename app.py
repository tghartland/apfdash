import os
import os.path
import urllib.parse

import dash
import flask

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
CSS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'css')

URL_PREFIX = os.environ.get("APFDASH_URL_PREFIX", "/")
if not URL_PREFIX.endswith("/"):
    URL_PREFIX = URL_PREFIX + "/"

def prefixed_url(url):
    if url.startswith("/"):
        # if url is intended to be prefixed, it should not start with a "/" anyway
        # and is likely a mistake
        url = url[1:]
    return urllib.parse.urljoin(URL_PREFIX, url)

app = dash.Dash(url_base_pathname=URL_PREFIX)
server = app.server
app.config.suppress_callback_exceptions = True
app.config.requests_pathname_prefix = URL_PREFIX
# app.scripts.config.serve_locally = True

@app.server.route(prefixed_url("scripts/<resource>"))
def serve_script(resource):
    if resource == "url_prefix.js":
        return "var URL_PREFIX = '{}'".format(URL_PREFIX)
    return flask.send_from_directory(SCRIPT_PATH, resource)

@app.server.route(prefixed_url("css/<resource>"))
def serve_css(resource):
    return flask.send_from_directory(CSS_PATH, resource)

# jQuery
app.scripts.append_script({"external_url": "https://code.jquery.com/jquery-3.2.1.slim.min.js"})

# for bootstrap
#app.scripts.append_script({"external_url": "https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js"})

# used for selecting elements in page as they are initialised
app.scripts.append_script({"external_url": prefixed_url("scripts/jquery.initialize.min.js")})

app.scripts.append_script({"external_url": prefixed_url("scripts/url_prefix.js")})
app.scripts.append_script({"external_url": prefixed_url("scripts/index.js")})

# dash example css and own css overrides
app.css.append_css({"external_url": prefixed_url("css/dash_base.css")})

# custom bootstrap css/js with only panels enabled
# from https://getbootstrap.com/docs/3.3/customize
app.scripts.append_script({"external_url": prefixed_url("scripts/bootstrap.js")})
app.css.append_css({"external_url": prefixed_url("css/bootstrap.css")})

# my css is loaded last so that it has the final say on overriding things
app.css.append_css({"external_url": prefixed_url("css/index.css")})
