from __future__ import annotations
from dataclasses import dataclass, field

from typing import List, Optional


@dataclass
class Genre:
    Id: str
    name: str


@dataclass
class Movie:
    Id: str
    name: str
    genres: List[str] = field(default_factory=lambda: [])


@dataclass
class User:
    Id: str
    name: str
    age: Optional[int] = None
    has_rated: List[Rating] = field(default_factory=lambda: [])

    @dataclass
    class Rating:
        movie: str
        score: int
