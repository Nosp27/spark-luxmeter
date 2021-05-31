from dash.dependencies import Input

from frontend.components import Component
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc


class AlertSettings(Component):
    def render(self):
        return html.Div(
            [
                html.Table(
                    *self.alert_endpoint()
                )
            ]
        )

    def alert_endpoint(self):
        return [
            html.Tr(html.H4("Alert Endpoint (POST request hook)")),
            html.Tr(
                html.Td(dcc.Input(debounce=True, id="alert-input")), html.Td(dbc.Button("Test", id="test-btn", color="secondary")),
                html.Td(id="alert-status-container", children=[dbc.Alert(is_open=False)]),
            )
        ]

    def add_callbacks(self, app):
        @app.callback(
            Input("test-btn", "n_clicks"),
            Input("alert-input", "value"),
        )
        def register_alert(n):
            pass

