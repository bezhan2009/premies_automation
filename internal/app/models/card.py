from dataclasses import dataclass
from datetime import date


@dataclass
class Card:
    expire_date: date
    issue_date: date
    card_type: str
    code: str
    in_balance: float
    debt_osd: float
    debt_osk: float
    out_balance: float
    owner_name: str
    coast: float
