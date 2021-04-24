import random
import re
from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input

from frontend.components.abc import Component


class TaskList(Component):
    def __init__(self):
        self.tasks = None

    def generate_sample_data(self, size=10):
        return [
            (f"Task_{i}", f"task_{i}", "nosp27", datetime.now() - timedelta(seconds=random.randint(120, 600))) for i in
            range(size)
        ]

    def compose_layout(self, tasks):
        return html.Div(
            children=[
                html.Div(
                    style=dict(display="flex", justifyContent="flex-start", alignItems="center"),
                    children=[
                        html.P("Yarn Host", style=dict(height="30px", width="100px", margin="10px")),
                        dcc.Input(
                            style=dict(width="700px"),
                            id="yarn-hostname",
                            type="text",
                            debounce=True,
                        ),
                        dbc.Alert(id="hostname-alert", is_open=False, style=dict(margin="15px"), color="danger"),
                        dbc.ButtonGroup(
                            id="host-accept-buttons",
                            style=dict(margin="10px", display="none"),
                            children=[
                                dbc.Button("✓", outline=True, color="success"),
                                dbc.Button("✕", outline=True, color="danger")
                            ],
                        )
                    ]
                ),
                html.Div(
                    style=dict(display="flex", justifyContent="flex-start", alignItems="center"),
                    children=[
                        html.P("Applications", style=dict(height="30px", width="100px", margin="10px")),
                        dcc.Dropdown(
                            style=dict(width="700px"),
                            id="app-list",
                            options=[
                                dict(label=f"{task_title} by {task_owner} since {task_start_time}", value=task_id,
                                     title=task_id)
                                for task_title, task_id, task_owner, task_start_time in tasks
                            ],
                            clearable=False,
                            value=tasks[0][1],
                        )
                    ]),
            ]
        )

    def add_callbacks(self, app):
        @app.callback(
            Output("store", "data"),
            Input("app-list", "value"),
            prevent_initial_call=True,
        )
        def select_application(data, value):
            data["app_id"] = value
            return data

        @app.callback(
            Output("yarn-hostname", "style"),
            Output("hostname-alert", "is_open"),
            Output("hostname-alert", "children"),
            Output("host-accept-buttons", "style"),
            Input("yarn-hostname", "value"),
            Input("yarn-hostname", "style"),
            Input("host-accept-buttons", "style"),
            prevent_initial_call=True,
        )
        def cb1(yarn_host: str, text_input_style, host_accept_buttons_style):
            if not yarn_host.startswith(("http://", "https://")):
                yarn_host = f"http://{yarn_host}"
            url_regex = re.compile(
                r'^(?:http)s?://'  # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
                r'localhost|'  # localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                r'(?::\d+)?'  # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            try:
                if re.match(url_regex, yarn_host) is None:
                    raise ValueError("Failed to parse hostname")

                # try:
                #     resp = requests.get(URL(yarn_host) / "api/v1/applications")
                # except requests.exceptions.RequestException as exc:
                #     raise ValueError(str(exc))
                # if resp.status_code != 200:
                #     raise ValueError()
            except ValueError as err:
                text_input_style["borderColor"] = "red"
                host_accept_buttons_style["display"] = "none"
                return text_input_style, True, str(err), host_accept_buttons_style
            else:
                text_input_style["borderColor"] = "lightgray"
                host_accept_buttons_style["display"] = "block"
                return text_input_style, False, "", host_accept_buttons_style

