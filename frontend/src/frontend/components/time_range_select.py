import dash
import dash_html_components as html
from dash.exceptions import PreventUpdate
from dash_core_components import Input as htmlInput
from dash.dependencies import Input, Output, State

from frontend.components import Component

import dash_core_components as dcc
from datetime import date, datetime, timedelta


class TimeRangeComponent(Component):
    def translate_time(self, dt):
        return ":".join(map(str, dt.timetuple()[3:5]))

    def render(self):
        return html.Div(
            [
                dcc.Store(
                    "plots-time-range",
                    data=tuple([datetime.now() - timedelta(hours=1), datetime.now()]),
                ),
                dcc.Dropdown(
                    options=[
                        {"value": x, "label": x} for x in ("Interval", "Last N hours")
                    ],
                    style={"width": "200px", "margin": "3px"},
                    value="Interval",
                    clearable=False,
                    id="tr-mode-dropdown",
                ),
                html.Div(
                    self.render_for_interval(),
                    id="trc-container-1",
                    style={"display": "flex"},
                ),
                html.Div(
                    self.render_for_last_hours(),
                    id="trc-container-2",
                    style={"display": "flex"},
                    hidden=True,
                ),
                html.Button("Apply", id="tr-apply-btn", style={"margin": "4px"}),
            ],
            style={"display": "flex"},
        )

    def render_for_interval(self):
        return [
            dcc.DatePickerRange(
                id="date-range",
                end_date=date.today(),
                display_format="MMM Do, YY",
                start_date_placeholder_text="MMM Do, YY",
                style={"margin": "4px"},
            ),
            self.input_component(
                "start-time", self.translate_time(datetime.now() - timedelta(hours=1))
            ),
            self.input_component("end-time", self.translate_time(datetime.now())),
        ]

    def render_for_last_hours(self):
        return [
            htmlInput(
                id="n-last-hours",
                value=1,
                min=1,
                max=24,
                inputMode="numeric",
                type="number",
                debounce=True,
                style={"margin": "4px"},
            )
        ]

    def input_component(self, id, default):
        return htmlInput(
            id=id,
            debounce=True,
            pattern="[0-9][0-9]?:[0-9][0-9]?",
            value=default,
            style={"margin": "4px"},
        )

    def add_callbacks(self, app):
        @app.callback(
            Output("trc-container-1", "hidden"),
            Output("trc-container-2", "hidden"),
            Input("tr-mode-dropdown", "value"),
        )
        def select_tr_mode(tr_mode):
            if tr_mode == "Interval":
                return False, True
            return True, False

        @app.callback(
            Output("plots-time-range", "data"),
            Output("start-time", "value"),
            Output("end-time", "value"),
            Input("tr-mode-dropdown", "value"),
            Input("tr-apply-btn", "n_clicks"),
            State("date-range", "start_date"),
            State("date-range", "end_date"),
            State("start-time", "value"),
            State("end-time", "value"),
            State("n-last-hours", "value"),
        )
        def apply_dt_range(
            tr_mode, _clicks, start_date, end_date, start_time_raw, end_time_raw, hours
        ):
            def parse_time(time_raw):
                hour, minute = [int(x) for x in time_raw.split(":")]
                hour = max(hour, 0)
                hour = min(hour, 23)
                minute = max(minute, 0)
                minute = min(minute, 59)
                return hour, minute

            if tr_mode == "Interval":
                try:
                    start_hour, start_minute = parse_time(start_time_raw)
                    end_hour, end_minute = parse_time(end_time_raw)
                except ValueError:
                    raise PreventUpdate()

                start_date = start_date or end_date
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
                end_date = datetime.strptime(end_date, "%Y-%m-%d")

                start_dt = start_date.replace(hour=start_hour, minute=start_minute)
                end_dt = end_date.replace(hour=end_hour, minute=end_minute)

                end_dt = min(end_dt, datetime.now().replace(microsecond=0, second=0))
                start_dt = min(start_dt, end_dt)
                start_hour, start_minute = start_dt.hour, start_dt.minute
                end_hour, end_minute = end_dt.hour, end_dt.minute
            else:
                end_dt = "now"
                start_dt = f"now-{hours}h"
                return (start_dt, end_dt), dash.no_update, dash.no_update

            return (
                (start_dt, end_dt),
                f"{start_hour}:{start_minute}",
                f"{end_hour}:{end_minute}",
            )
