import re

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash
from dash.dependencies import Input, Output

from frontend import data
from frontend.components.abc import Component


class HostSelector(Component):
    def render(self):
        default_hostname = data["base_url"]
        return html.Div(
            style=dict(
                display="flex", justifyContent="flex-start", alignItems="center"
            ),
            children=[
                html.P(
                    "Yarn Host",
                    style=dict(height="30px", width="100px", margin="10px"),
                ),
                dcc.Input(
                    style=dict(width="700px"),
                    id="yarn-hostname",
                    type="text",
                    debounce=True,
                    value=default_hostname,
                ),
                dbc.Alert(
                    id="hostname-alert",
                    is_open=False,
                    style=dict(margin="15px"),
                    color="danger",
                ),
                dbc.ButtonGroup(
                    id="host-accept-buttons",
                    style=dict(margin="10px", display="block"),
                    children=[
                        dbc.Button(
                            "✓", outline=True, color="success", id="host-confirm-btn",
                        ),
                        dbc.Button(
                            "✕", outline=True, color="danger", id="host-reject-btn",
                        ),
                    ],
                ),
            ],
        )

    def add_callbacks(self, app):
        # Hostname input

        def fill_host(yarn_host: str, text_input_style, host_accept_buttons_style):
            if not yarn_host.startswith(("http://", "https://")):
                yarn_host = f"http://{yarn_host}"
            url_regex = re.compile(
                r"^(?:http)s?://"  # http:// or https://
                r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|"  # domain...
                r"localhost|"  # localhost...
                r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
                r"(?::\d+)?"  # optional port
                r"(?:/?|[/?]\S+)$",
                re.IGNORECASE,
            )

            host_alert = False
            host_alert_content = ""
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
                # Error
                host_alert = True
                host_alert_content = str(err)
                text_input_style["borderColor"] = "red"
                host_accept_buttons_style["display"] = "none"
            else:
                # Done Successful
                text_input_style["borderColor"] = "lightgray"
                host_accept_buttons_style["display"] = "block"
            return (
                yarn_host,
                text_input_style,
                host_alert,
                host_alert_content,
                host_accept_buttons_style,
            )

        def confirm_host(style, yarn_host, confirmed, valid_hostname):
            style["display"] = "none"

            if confirmed:
                hostname = yarn_host
            elif valid_hostname:
                hostname = valid_hostname
            else:
                hostname = ""
            return style, hostname

        @app.callback(
            Output("host-accept-buttons", "style"),
            Output("hostname-info", "data"),
            Output("yarn-hostname", "value"),
            Output("hostname-alert", "is_open"),
            Output("hostname-alert", "children"),
            Output("tabs-container", "hidden"),
            Input("host-confirm-btn", "n_clicks"),
            Input("host-reject-btn", "n_clicks"),
            Input("yarn-hostname", "value"),
            Input("yarn-hostname", "style"),
            Input("host-accept-buttons", "style"),
            Input("hostname-info", "data"),
        )
        def fill_and_verdict_host_cb(
            _, __, hostname_input, hostname_style, btns_style, valid_hostname
        ):
            trigger_id, trigger_option = [
                p["prop_id"] for p in dash.callback_context.triggered
            ][0].split(".")

            show_alert = dash.no_update
            alert_content = dash.no_update
            hostname_output = dash.no_update
            hostname_input_validated = dash.no_update
            hidden_tabs = dash.no_update

            if "yarn-hostname" == trigger_id and trigger_option == "value":
                (
                    hostname_input_validated,
                    hostname_style,
                    show_alert,
                    alert_content,
                    btns_style,
                ) = fill_host(hostname_input, hostname_style, btns_style)
            elif trigger_id in ("host-confirm-btn", "host-reject-btn"):
                btns_style, hostname_output = confirm_host(
                    btns_style,
                    hostname_input,
                    trigger_id == "host-confirm-btn",
                    valid_hostname,
                )
                if not hostname_output:
                    hidden_tabs = True
                else:
                    hidden_tabs = False
                hostname_input_validated = hostname_output
            return (
                btns_style,
                hostname_output,
                hostname_input_validated,
                show_alert,
                alert_content,
                hidden_tabs,
            )
