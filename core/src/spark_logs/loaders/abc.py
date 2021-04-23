from spark_logs.loaders.fetchers import HttpFetcher
from spark_logs.loaders.parsers import AppIdsFromHtml, JsonParser


class MetricsLoader:
    def __init__(self):
        self.fetcher = HttpFetcher()
        self.application_list_parser = AppIdsFromHtml()
        self.json_parser = JsonParser()
        self.applications = dict()

    def load_application_info_list(self):
        data = self.application_list_parser.execute(
            self.fetcher.fetch(node="applications"), "ID"
        )
        data = {
            key: value for key, value in data.items() if value["State"] == "RUNNING"
        }
        self.applications = data

        return self.applications

    def load(self, application_id):
        ret = {}
        fetcher = self.fetcher

        # Load application data
        for key, index in zip(
            ["jobs", "stages", "executors"], ["jobId", "stageId", "id"]
        ):
            response = fetcher.fetch(node=key, application_id=application_id)
            ret[key] = self.json_parser.execute(response, index)
        return ret
