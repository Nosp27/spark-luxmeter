import abc
import pandas as pd


class HybridMetricStrategy(abc.ABC):
    @abc.abstractmethod
    def apply(self, df: pd.DataFrame):
        pass
