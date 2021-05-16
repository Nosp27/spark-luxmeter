import requests

loader_endpoint = "http://localhost:8001"
hybtid_metrics_endpoint = "http://localhost:10111"
anomaly_detection_endpoint = "http://localhost:10000"


def load_app_list():
    response = requests.get(",".join([loader_endpoint, "applications"]))
    response.raise_for_status()
    return response.json()


def toggle_app(app_id, on=True):
    _toggle_anomaly_detection(app_id, on)


def _toggle_loader(app_id, on):
    command = "create" if on else "rm"
    response = requests.post(
        "/".join([loader_endpoint, "client", command]), params={"app_id": app_id}, timeout=3
    )
    response.raise_for_status()
    return response.json()


def _toggle_anomaly_detection(app_id, on):
    command = "create" if on else "rm_app"
    response = requests.post(
        "/".join([loader_endpoint, "detector", command]), params={"app_id": app_id}, timeout=3
    )
    response.raise_for_status()
    return response.json()


def _toggle_hybrid_metrics(app_id, on):
    command = "create" if on else "rm_app"
    response = requests.post(
        "/".join([loader_endpoint, "metric", command]), params={"app_id": app_id}, timeout=3
    )
    response.raise_for_status()
    return response.json()
