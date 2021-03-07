import pandas as pd

from spark_luxmeter.spark_logs.loaders.fetchers import HttpFetcher
from spark_luxmeter.spark_logs.loaders.parsers import AppIdsFromHtml, JsonParser, AppInfoJsonParser


class BaseMetricsLoader:
    def __init__(self):
        self.fetcher = HttpFetcher()
        self.application_list_parser = AppIdsFromHtml()
        self.app_parser = AppInfoJsonParser()
        self.applications = dict()

    def load_application_ids(self):
        df = self.application_list_parser.execute(self.fetcher.fetch(node="applications"))
        df = df.loc[df["State"] == "RUNNING"]
        self.applications = df

        return self.applications

    def load(self, application_id) -> pd.DataFrame:
        ret = {}
        fetcher = self.fetcher

        # Load application data
        response = fetcher.fetch(node="application", application_id=application_id)
        ret["application_data"] = self.app_parser.execute(response)
        return ret
