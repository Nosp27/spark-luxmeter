from typing import Tuple, List, Set

import dash
import requests
from dash.dependencies import Output, Input

from frontend.components import Component, rest_api
import dash_html_components as html
import dash_core_components as dcc


class ConfigurationPage(Component):
    def __init__(self):
        self.application_list: List[Tuple[str, str]] = [
            ("application_1", "SOME APPLICATION"),
            ("application_2", "SOME ANOTHER APPLICATION"),
        ]
        self.inputs = []
        self.old_selected: Set[str] = set()

    def render(self):
        return html.Div(children=[self._create_app_selector()])

    def _create_app_selector(self):
        app_data = [{"label": app[1], "value": app[0]} for app in self.application_list]
        self.inputs = [f"{ad['value']}-item" for ad in app_data]
        return dcc.Checklist(
            options=app_data,
            inputStyle={"margin": "10px"},
            id="applications-checklist",
        )

    def add_callbacks(self, app):
        @app.callback(
            Output("applications-checklist", "value"),
            Input("applications-checklist", "value"),
        )
        def cb(list_selected):
            if not list_selected:
                return dash.no_update
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

            for el in new_deselected:
                try:
                    rest_api.toggle_app(el, on=False)
                    result_selected.remove(el)
                except requests.RequestException as exc:
                    pass

            self.old_selected = result_selected

            return tuple(result_selected)
