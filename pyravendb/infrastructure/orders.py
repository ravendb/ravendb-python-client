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
