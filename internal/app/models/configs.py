from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    name: str


@dataclass
class GrpcConfig:
    host: str
    port: int
    max_workers: int


@dataclass
class DefExcelPaths:
    def_card_prices: str
    def_report_template_cards: str
    def_report_template_credits: str


@dataclass
class DefOutPaths:
    output_dir_reports: str
    zip_file_path: str


@dataclass
class AutomationDetails:
    def_excel_paths: DefExcelPaths
    def_out_paths: DefOutPaths


@dataclass
class Config:
    database: DatabaseConfig
    grpc: GrpcConfig
    automation_details: AutomationDetails
