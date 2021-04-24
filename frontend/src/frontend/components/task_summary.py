from datetime import datetime, timedelta
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_daq as daq
import dash_html_components as html

from frontend.components.abc import Component


class TaskSummary(Component):
    def __init__(self):
        self.task_state = self.update_state()

    def random_data(self):
        pass

    def random_plot(self):
        pass

    def plot(self):
        return dbc.Container([
            dbc.Row([
                dbc.Col(dbc.Card([html.H2(f"{k}"), html.P(v)], body=True))
                for k, v in self.task_state.items()
            ]),
        ], fluid=False)

    def update_state(self):
        return dict(
            cpu_utilization=0.3,
            shuffle=0.6,
            # runtime=timedelta(hours=3, minutes=14, seconds=38),
        )
