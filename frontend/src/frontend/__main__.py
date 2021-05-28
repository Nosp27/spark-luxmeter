import dash_core_components as dcc
import dash_html_components as html

from frontend import app
from frontend.components import MemoryPlot, AppSummary, TaskList
from frontend.components.configuration import ConfigurationPage
from frontend.components.graphs.graphite_graph import GraphiteGraphComponent
from frontend.components.host_selector import HostSelector
from frontend.components.time_range_select import TimeRangeComponent

components = dict(
    host_selector=HostSelector(),
    mem_plot=MemoryPlot(),
    task_selector=TaskList(),
    tasks_summary=AppSummary(uid="tasks-summary"),
    configuration=ConfigurationPage(),
    anomaly_graph=GraphiteGraphComponent(),
    time_range=TimeRangeComponent(),
)


def create_configuration():
    return html.Div([components["configuration"].render()])


def create_monitoring_data():
    return [
        html.Div(
            children=[
                components["time_range"].render(),
                html.Div(
                    [
                        html.Div(
                            [
                                components["mem_plot"].render(),
                                components["anomaly_graph"].render(),
                            ]
                        ),
                        html.Div([components["tasks_summary"].render()]),
                    ],
                    style={"display": "flex"},
                ),
            ],
        )
    ]


def create_components():
    return html.Div(
        id="components",
        children=[
            html.Div(
                children=[
                    html.Div(
                        id="data-for-known-host",
                        children=[
                            components["task_selector"].render(),
                            html.Div(
                                id="data-for-known-app",
                                children=create_monitoring_data(),
                            ),
                        ],
                    ),
                ]
            )
        ],
    )


if __name__ == "__main__":
    app.layout = html.Div(
        [
            components["host_selector"].render(),
            html.Div(
                id="tabs-container",
                hidden=True,
                children=[
                    dcc.Tabs(
                        id="tabs",
                        style={"marginTop": "10px"},
                        children=[
                            dcc.Tab(
                                label="Configuration",
                                children=[components["configuration"].render()],
                                className="custom-tab",
                                selected_className="custom-tab--selected",
                            ),
                            dcc.Tab(
                                label="Visualization",
                                children=[create_components()],
                                className="custom-tab",
                                selected_className="custom-tab--selected",
                            ),
                        ],
                    )
                ],
            ),
            dcc.Store(id="selected-app-info", data=dict()),
            dcc.Store(id="hostname-info", data=""),
            dcc.Store(id="app-list-info", data=[]),
            dcc.Store(id="applications-actually-processing", data=[]),
            dcc.Interval(id="interval", interval=3000),
            dcc.Interval("service-check-interval", interval=300),
        ]
    )

    for component in components.values():
        component.add_callbacks(app)

    app.run_server(debug=True)
