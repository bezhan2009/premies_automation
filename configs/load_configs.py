from typing import Type

import yaml

from internal.app.models.configs import (
    DatabaseConfig,
    GrpcConfig,
    DefExcelPaths,
    DefOutPaths,
    AutomationDetails,
    Config
)

config = Config


def load_config(path: str):
    with open(path, 'r') as f:
        global config
        raw = yaml.safe_load(f)

        automation = raw['automation_details']
        automation_details = AutomationDetails(
            def_excel_paths=DefExcelPaths(**automation['def_excel_paths']),
            def_out_paths=DefOutPaths(**automation['def_out_paths'])
        )

        config = Config(
            database=DatabaseConfig(**raw['database']),
            grpc=GrpcConfig(**raw['grpc']),
            automation_details=automation_details
        )


def get_config() -> Type[Config]:
    global config

    return config
