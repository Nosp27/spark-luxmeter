import json
from typing import Any, Dict


class ConfigLoader:
    def load_config(self) -> Dict[str, Any]:
        pass


class LocalFileConfigLoader(ConfigLoader):
    def __init__(self, filename="/usr/local/share/spark-luxmeter/config.json"):
        self.filename = filename

    def load_config(self):
        return json.load(open(self.filename))


class Config:
    def __init__(self, loader):
        super().__init__()
        assert issubclass(loader.__class__, ConfigLoader)
        self.loader = loader
        self.config = None

    def ensure_config(self):
        if self.config is None:
            try:
                self.config = self.loader.load_config()
            except FileNotFoundError:
                self.config = dict()

    def __getitem__(self, key):
        self.ensure_config()
        return self.config[key]

    def get(self, key):
        self.ensure_config()
        return self.config.get(key)


DEFAULT_CONFIG = Config(LocalFileConfigLoader())
