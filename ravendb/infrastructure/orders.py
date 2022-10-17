import datetime
from typing import List


class Contact:
    def __init__(self, name: str = None, title: str = None):
        self.name = name
        self.title = title


class Address:
    def __init__(
        self,
        line1: str = None,
        line2: str = None,
        city: str = None,
        region: str = None,
        postal_code: str = None,
        country: str = None,
    ):
        self.line1 = line1
        self.line2 = line2
        self.city = city
        self.region = region
        self.postal_code = postal_code
        self.country = country


class Company:
    def __init__(
        self,
        Id: str = None,
        external_id: str = None,
        name: str = None,
        contact: Contact = None,
        address: Address = None,
        phone: str = None,
        fax: str = None,
    ):
        self.Id = Id
        self.external_id = external_id
        self.name = name
        self.contact = contact
        self.address = address
        self.phone = phone
        self.fax = fax


class Employee:
    def __init__(
        self,
        Id: str = None,
        last_name: str = None,
        first_name: str = None,
        title: str = None,
        address: Address = None,
        hired_at: datetime.datetime = None,
        birthday: datetime.datetime = None,
        home_phone: str = None,
        extension: str = None,
        reports_to: str = None,
        notes: List[str] = None,
        territories: List[str] = None,
    ):
        self.Id = Id
        self.last_name = last_name
        self.first_name = first_name
        self.title = title
        self.address = address
        self.hired_at = hired_at
        self.birthday = birthday
        self.home_phone = home_phone
        self.extension = extension
        self.reports_to = reports_to
        self.notes = notes
        self.territories = territories


class Product:
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        supplier: str = None,
        category: str = None,
        quantity_per_unit: str = None,
        price_per_unit: float = None,
        units_in_stock: int = None,
        units_on_order: int = None,
        discontinued: bool = None,
        reorder_level: int = None,
    ):
        self.Id = Id
        self.name = name
        self.supplier = supplier
        self.category = category
        self.quantity_per_unit = quantity_per_unit
        self.price_per_unit = price_per_unit
        self.units_in_stock = units_in_stock
        self.units_on_order = units_on_order
        self.discontinued = discontinued
        self.reorder_level = reorder_level


class OrderLine(object):
    def __init__(
        self,
        product: str = None,
        product_name: str = None,
        price_per_unit: float = None,
        quantity: int = None,
        discount: float = None,
    ):
        self.product = product
        self.product_name = product_name
        self.price_per_unit = price_per_unit
        self.quantity = quantity
        self.discount = discount


class Order:
    def __init__(
        self,
        key: str = None,
        company: str = None,
        employee: str = None,
        ordered_at: datetime.datetime = None,
        require_at: datetime.datetime = None,
        shipped_at: datetime.datetime = None,
        ship_to: Address = None,
        ship_via: str = None,
        freight: float = None,
        lines: List[OrderLine] = None,
    ):
        self.key = key
        self.company = company
        self.employee = employee
        self.ordered_at = ordered_at
        self.require_at = require_at
        self.shipped_at = shipped_at
        self.ship_to = ship_to
        self.ship_via = ship_via
        self.freight = freight
        self.lines = lines


class Category:
    def __init__(self, Id: str = None, name: str = None, description: str = None):
        self.Id = Id
        self.name = name
        self.description = description


class Supplier:
    def __init__(
        self,
        Id: str = None,
        contact: Contact = None,
        name: str = None,
        address: Address = None,
        phone: str = None,
        fax: str = None,
        homepage: str = None,
    ):
        self.Id = Id
        self.contact = contact
        self.name = name
        self.address = address
        self.phone = phone
        self.fax = fax
        self.homepage = homepage
