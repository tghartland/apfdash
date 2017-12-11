import dash

app = dash.Dash()
server = app.server
app.config.suppress_callback_exceptions = True
# app.scripts.config.serve_locally = True

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

app.scripts.append_script({"external_url": "https://code.jquery.com/jquery-3.2.1.min.js"})
app.scripts.append_script({"external_url": "https://pastebin.com/raw/DiXVQWvc"})