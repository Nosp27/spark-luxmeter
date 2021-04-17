from spark_luxmeter.spark_logs.metrics import MetricsClient


class ApplicationFetchTask:
    def __init__(self, metrics_client, app_id):
        self.app_id = app_id
        self.metrics_client: MetricsClient = metrics_client
        self.app_metrics = []

    async def append_app_metrics(self):
        self.metrics_client
