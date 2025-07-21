from dataclasses import dataclass


@dataclass
class MobileBank:
    connects: int
    prem: float
    owner_name: str
