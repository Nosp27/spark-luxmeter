from spark_luxmeter.spark_logs.loaders.fetchers import HttpFetcher
from spark_luxmeter.spark_logs.loaders.parsers import AppIdsFromHtml, JsonParser


class MetricsLoader:
    def __init__(self):
        self.fetcher = HttpFetcher()
        self.application_list_parser = AppIdsFromHtml()
        self.json_parser = JsonParser()
        self.applications = dict()

    def load_application_info_list(self):
        data = self.application_list_parser.execute(self.fetcher.fetch(node="applications"))
        data = [row for row in data if row["State"] == "RUNNING"]
        self.applications = data

        return self.applications

    def load(self, application_id):
        ret = {}
        fetcher = self.fetcher

        # Load application data
        for key in ["jobs", "stages", "executors"]:
            response = fetcher.fetch(node=key, application_id=application_id)
            ret[key] = self.json_parser.execute(response)
        return ret
