import requests

from spark_logs.config import DEFAULT_CONFIG

services_config = DEFAULT_CONFIG["services"]


def toggle_app(app_id, on=True):
    _toggle_anomaly_detection(app_id, on)


def _toggle_loader(app_id, on):
    command = "create" if on else "rm"
    response = requests.post(
        "/".join([services_config["loader"]["endpoint"], "client", command]),
        params={"app_id": app_id},
        timeout=3,
    )
    response.raise_for_status()
    return response.json()


def _toggle_anomaly_detection(app_id, on):
    command = "create" if on else "rm"
    response = requests.post(
        "/".join([services_config["anomaly_detector"]["endpoint"], "detector", command]),
        params={"app_id": app_id, "metric_name": "sequential_processor"},
        timeout=3,
    )
    response.raise_for_status()
    return response.json()


def _toggle_hybrid_metrics(app_id, on):
    command = "create" if on else "rm"
    response = requests.post(
        "/".join([services_config["hybrid_metrics"]["endpoint"], "metric", command]),
        params={"app_id": app_id},
        timeout=3,
    )
    response.raise_for_status()
    return response.json()
