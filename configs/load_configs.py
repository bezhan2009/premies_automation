from typing import Type

import yaml

from internal.app.models.configs import (
    DatabaseConfig,
    GrpcConfig,
    Config
)

config = Config


def load_config(path: str):
    with open(path, 'r') as f:
        global config
        raw = yaml.safe_load(f)
        config = Config(
            database=DatabaseConfig(**raw['database']),
            grpc=GrpcConfig(**raw['grpc'])
        )


def get_config() -> Type[Config]:
    global config

    return config
