from typing import Tuple, List, Set

import dash
import orjson
import requests
from dash.dependencies import Output, Input

from frontend import kvstore
from frontend.components import Component, rest_api
import dash_html_components as html
import dash_core_components as dcc

from frontend.components.processors_check import ProcessorChecks
from frontend.components.service_checks import ServiceCheck


class ConfigurationPage(Component):
    def __init__(self):
        self.application_list: List[Tuple[str, str]] = []
        self.old_selected: Set[str] = set()
        self.serivce_check = ServiceCheck()
        self.processor_check = ProcessorChecks()

    def render(self):
        return html.Div(
            children=[
                self._create_app_selector([]), self.serivce_check.render(), self.processor_check.render(), dcc.Store(id="processing-applications-info")
            ],
            style={"display": "flex"},
        )

    def _create_app_selector(self, app_data):
        return dcc.Checklist(
            options=app_data,
            inputStyle={"margin": "10px"},
            style={"height": "600px", "width": "400px", "border": "solid black"},
            className="overflow-auto",
            id="applications-checklist",
        )

    def add_callbacks(self, app):
        @app.callback(
            Output("processing-applications-info", "data"),
            Output("applications-checklist", "value"),
            Input("applications-checklist", "value"),
            Input("processing-applications-info", "data"),
            Input("applications-actually-processing", "data")
        )
        def toggle_selection(list_selected, processing_app_list, actually_processing):
            list_selected = list_selected or []
            set_selected = set(list_selected)
            new_selected = set_selected - self.old_selected
            new_deselected = self.old_selected - set_selected

            result_selected = self.old_selected.copy()

            for el in new_selected:
                try:
                    rest_api.toggle_app(el, on=True)
                    result_selected.add(el)
                except requests.RequestException as exc:
                    pass

            removed = set()
            for el in new_deselected:
                try:
                    rest_api.toggle_app(el, on=False)
                    removed.add(el)
                    result_selected.remove(el)
                except requests.RequestException as exc:
                    pass

            result_selected.update(set(actually_processing) - removed)
            self.old_selected = result_selected

            return tuple(result_selected), tuple(result_selected)

        @app.callback(
            Output("applications-checklist", "options"), Input("app-list-info", "data")
        )
        def refresh_application_checklist(apps):
            if not apps:
                return dash.no_update
            app_data = [{"label": a, "value": a} for a in apps]
            return app_data

        self.serivce_check.add_callbacks(app)
        self.processor_check.add_callbacks(app)
