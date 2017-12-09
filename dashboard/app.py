import dash

app = dash.Dash()
server = app.server
app.config.suppress_callback_exceptions = True
app.scripts.config.serve_locally = True

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

print(dir(app.css))