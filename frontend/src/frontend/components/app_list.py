import random
import re
from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import orjson
import dash
from dash.dependencies import Output, Input, State

from frontend.components import rest_api
from frontend.components.abc import Component


class TaskList(Component):
    def __init__(self):
        self.tasks = None

    def generate_sample_data(self, size=10):
        return [
            (
                f"Task_{i}",
                f"task_{i}",
                "nosp27",
                datetime.now() - timedelta(seconds=random.randint(120, 600)),
            )
            for i in range(size)
        ]

    def render(self, tasks):
        return html.Div(
            children=[
                html.Div(
                    style=dict(
                        display="flex", justifyContent="flex-start", alignItems="center"
                    ),
                    children=[
                        html.P(
                            "Applications",
                            style=dict(margin="10px"),
                        ),
                        dcc.Dropdown(
                            style=dict(width="700px"),
                            id="app-list",
                            options=[
                                dict(
                                    label=f"{task_title} by {task_owner} since {task_start_time}",
                                    value=task_id,
                                    title=task_id,
                                )
                                for task_title, task_id, task_owner, task_start_time in tasks
                            ],
                            clearable=False,
                            value=tasks[0][1],
                        ),
                    ],
                ),
            ]
        )

    def add_callbacks(self, app):
        @app.callback(
            Input("interval", "value"),
            State("hostname-info", "data"),
            Output("app-list-info", "data"),
            Output("app-list-alert", "value"),
            Output("app-list-alert", "is_open"),
        )
        def load_app_list(_, hostname_info):
            if not hostname_info:
                return [], "", False
            apps = []
            try:
                apps = rest_api.load_app_list()
            except Exception as exc:
                # Alert
                alert_mode = True
                alert_text = str(exc)
            else:
                alert_mode = False
                alert_text = ""
            return apps, alert_text, alert_mode
