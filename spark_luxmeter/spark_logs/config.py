import json
from typing import Any, Dict


class ConfigLoader:
    def load_config(self) -> Dict[str, Any]:
        pass


class LocalFileConfigLoader(ConfigLoader):
    def __init__(self, filename="config.json"):
        self.filename = filename

    def load_config(self):
        return json.load(open(self.filename))


class Config:
    def __init__(self, loader):
        super().__init__()
        assert issubclass(loader.__class__, ConfigLoader)
        self.loader = loader
        self.config = self.loader.load_config()

    def __getitem__(self, key):
        return self.config[key]

    def get(self, key):
        return self.config.get(key)


CONFIG = Config(LocalFileConfigLoader())
