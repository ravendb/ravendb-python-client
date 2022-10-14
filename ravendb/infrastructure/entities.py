from __future__ import annotations
from datetime import datetime
from typing import List, Dict


class User:
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        last_name: str = None,
        address_id: str = None,
        count: int = None,
        age: int = None,
    ):
        self.Id = Id
        self.name = name
        self.last_name = last_name
        self.address_id = address_id
        self.count = count
        self.age = age


class Post:
    def __init__(
        self,
        Id: str = None,
        title: str = None,
        desc: str = None,
        comments: List[Post] = None,
        attachment_ids: str = None,
        created_at: datetime.date = None,
    ):
        self.Id = Id
        self.title = title
        self.desc = desc
        self.comments = comments
        self.attachment_ids = attachment_ids
        self.created_at = created_at
