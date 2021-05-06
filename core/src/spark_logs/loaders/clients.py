from spark_logs.loaders.fetchers import HttpFetcher
from spark_logs.loaders.parsers import (
    AppIdsFromHtml,
    JsonParser,
    StageExtendedParser,
    Jobs,
)


class MetricsClient:
    def __init__(self, *, inactive_jobs_only):
        self.fetcher = HttpFetcher()
        self.parsers = {
            "applications": AppIdsFromHtml(),
            "jobs": Jobs(inactive_only=inactive_jobs_only),
            "stage": StageExtendedParser(),
        }
        self.default_parser = JsonParser()

    async def get_node_metrics(self, node, **data):
        parser = self.parsers.get(node) or self.default_parser
        response = await self.fetcher.fetch(
            node=node, resp_format=parser.resp_format, **data
        )
        print("Got response")
        return parser.execute(response)
