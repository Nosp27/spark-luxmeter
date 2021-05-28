import dash
import requests
from dash.dependencies import Output, Input

from frontend.components import Component
import dash_daq as daq
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from spark_logs.config import DEFAULT_CONFIG

SERVICES = DEFAULT_CONFIG.get("services")


class ServiceCheck(Component):
    def render(self):
        return html.Div(
            [
                daq.GraduatedBar(
                    id="healthcheck-loading-bar",
                    min=1,
                    value=1,
                    max=10,
                    step=1,
                    size=400,
                ),
                dbc.Card(
                    id="service-checks-dashboard",
                    children=[],
                    style={"width": "400px"},
                ),
            ],
            style={"margin": "30px"},
        )

    def _render_content(self, service_checks):
        return [
            dbc.CardBody(
                dbc.ListGroup(
                    [
                        dbc.ListGroupItem(
                            [
                                html.H5(
                                    service["name"],
                                    style={
                                        "text-align": "start",
                                        "margin-right": "30px",
                                    },
                                ),
                                html.P(
                                    service["status"],
                                    style={"paddingTop": "10px", "margin-end": "30px"},
                                ),
                                daq.Indicator(
                                    id=f"{service['name'].lower().replace(' ', '-')}-health-indicator",
                                    color=service["color"],
                                    style={
                                        "margin": "15px",
                                        "vertical-align": "middle",
                                    },
                                ),
                            ],
                            style={
                                "display": "flex",
                                "flex-direction": "row",
                                "justify-content": "flex-end",
                            },
                        )
                        for service in service_checks
                    ]
                ),
            )
        ]

    def add_callbacks(self, app):
        @app.callback(
            Output("service-checks-dashboard", "children"),
            Output("healthcheck-loading-bar", "value"),
            Input("service-check-interval", "n_intervals"),
        )
        def make_healthchecks(n):
            def one_healthcheck(service):
                addr = service["endpoint"]
                try:
                    resp = requests.get(addr)
                except requests.exceptions.RequestException as exc:
                    return "cannot reach host"

                try:
                    resp.raise_for_status()
                    return resp.json()["status"]
                except requests.exceptions.RequestException as exc:
                    return f"{resp.status_code} received"

            n = n or 0
            value = n % 10 + 1

            if value == 1:
                color_table = {
                    "unknown": "gray",
                    "healthy": "green",
                }

                services = [
                    {"name": service["name"], "status": one_healthcheck(service)}
                    for service in SERVICES.values()
                ]
                for x in services:
                    x["color"] = color_table.get(x["status"], "red")
                return self._render_content(services), value
            return dash.no_update, value
