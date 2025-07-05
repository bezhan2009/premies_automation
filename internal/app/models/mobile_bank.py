from dataclasses import dataclass


@dataclass
class MobileBank:
    inn: str
    prem: float
    owner_name: str
