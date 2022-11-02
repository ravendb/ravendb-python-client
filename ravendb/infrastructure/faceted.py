from __future__ import annotations
import datetime
from enum import Enum
from typing import Dict


class Currency(Enum):
    EUR = "EUR"
    PLN = "PLN"
    NIS = "NIS"


class Order:
    def __init__(
        self,
        product: str = None,
        total: float = None,
        currency: Currency = None,
        quantity: int = None,
        region: int = None,
        at: datetime.datetime = None,
        tax: float = None,
    ):
        self.product = product
        self.total = total
        self.currency = currency
        self.quantity = quantity
        self.region = region
        self.at = at
        self.tax = tax

    @classmethod
    def from_json(cls, json_dict: Dict) -> Order:
        return cls(
            json_dict["product"],
            json_dict["total"],
            Currency(json_dict["currency"]),
            json_dict["quantity"],
            json_dict["region"],
            datetime.datetime.fromisoformat(json_dict["at"]) if "at" in json_dict else None,
            json_dict["tax"],
        )

    def to_json(self) -> Dict:
        return {
            "product": self.product,
            "total": self.total,
            "currency": self.currency.value,
            "quantity": self.quantity,
            "region": self.region,
            "at": self.at.isoformat() if self.at else None,
            "tax": self.tax,
        }
