import itertools

import dash
import dash_bootstrap_components as dbc
import dash_daq as daq
import dash_html_components as html
import requests
from dash.dependencies import Output, Input

from frontend.components import Component
from spark_logs.config import DEFAULT_CONFIG


class ProcessorChecks(Component):
    def render(self):
        return html.Div([], style={"margin": "30px"}, id="processors-checks-dashboard")

    def _render_loader_checks(self, loader_checks):
        color_map = {"done": "blue", "running": "green"}

        component_cards = []
        for component, data in loader_checks.items():
            elements = [html.H4(component.capitalize())]
            for app_id, services in data.items():
                elements.append(html.P(app_id, style={"fontSize": "12pt"}))
                for service_name, tasks in services.items():
                    item = dbc.ListGroupItem(
                        [
                            html.P(
                                service_name,
                                style={
                                    "fontSize": "10pt",
                                    "marginTop": "10px",
                                    "marginRight": "70px",
                                },
                            ),
                            html.Div(
                                [
                                    daq.Indicator(
                                        id=f"{service_name.lower().replace(' ', '-')}-health-indicator-{task_idx}",
                                        color=color_map.get(status, "red"),
                                        style={
                                            "margin": "15px",
                                            "vertical-align": "middle",
                                        },
                                    )
                                    for task_idx, status in enumerate(tasks.values())
                                ],
                                style={"display": "flex"},
                            ),
                        ],
                        style={
                            "display": "flex",
                            "flex-direction": "row",
                            "justify-content": "space-between",
                        },
                    )
                    elements.append(item)
            component_cards.append(
                dbc.Card(
                    id=f"processor-checks-dashboard-{component}",
                    children=elements,
                    style={"width": "400px", "margin": "30px", "padding": "10px"},
                )
            )
        return component_cards

    def add_callbacks(self, app):
        @app.callback(
            Output("applications-actually-processing", "data"),
            Output("processors-checks-dashboard", "children"),
            Input("service-check-interval", "n_intervals"),
        )
        def ls_processors(n):
            if n and n % 10 != 0:
                return dash.no_update, dash.no_update

            services_config = DEFAULT_CONFIG["services"]
            total_data = dict()

            for service_name, config in services_config.items():
                endpoint = config["endpoint"]
                try:
                    resp = requests.get(f"{endpoint}/client/ls")
                    resp.raise_for_status()
                    processors = resp.json()["applications"]
                except requests.exceptions.RequestException as exc:
                    processors = dict()
                except Exception as exc:
                    raise
                total_data[service_name] = processors

            applications_actually_processing = {
                app_id
                for app_id in itertools.chain.from_iterable(total_data.values())
                if app_id.startswith("application_")
            }

            return (
                tuple(applications_actually_processing),
                self._render_loader_checks(total_data),
            )
