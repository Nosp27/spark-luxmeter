import dash_html_components as html
import dash_core_components as dcc

from frontend import app
from frontend.components import MemoryPlot, AppSummary, TaskList
from frontend.components.configuration import ConfigurationPage
from frontend.components.host_selector import HostSelector


components = dict(
    host_selector=HostSelector(),
    mem_plot=MemoryPlot(),
    task_selector=TaskList(),
    tasks_summary=AppSummary(),
    configuration=ConfigurationPage(),
)


def create_configuration():
    return html.Div([components["configuration"].render()])


def create_components():
    return html.Div(
        id="components",
        children=[
            html.Div(
                children=[
                    html.Button(
                        "Regenerate", "regenerate-btn", style=dict(display="block"),
                    ),
                    html.Div(
                        id="data-for-known-host",
                        children=[
                            components["task_selector"].render(
                                components["task_selector"].generate_sample_data()
                            ),
                            html.Div(
                                id="data-for-known-app",
                                children=[
                                    components["mem_plot"].random_plot(),
                                    components["tasks_summary"].plot(),
                                ],
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
                ]
            ),
            dcc.Store(id="store", data=dict()),
            dcc.Store(id="hostname-info", data=""),
            dcc.Interval(id="interval", interval=30),
        ]
    )

    for component in components.values():
        component.add_callbacks(app)

    app.run_server(debug=True)
