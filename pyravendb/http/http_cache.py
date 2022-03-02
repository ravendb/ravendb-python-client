import datetime
from enum import Enum
from typing import Union, Optional, Dict, Set


class ItemFlags(Enum):
    NONE = "None"
    NOT_FOUND = "NotFound"
    AGGRESSIVELY_CACHED = "AggressivelyCached"

    def __str__(self):
        return self.value


class HttpCacheItem:
    def __init__(self):
        self.change_vector: Union[None, str] = None
        self.payload: Union[None, str] = None
        self.last_server_update: datetime.datetime = datetime.datetime.now()
        self.flags: Set[ItemFlags] = {ItemFlags.NONE}
        self.generation: Union[None, int] = None

        self.cache: Union[None, HttpCache] = None


class ReleaseCacheItem:
    def __init__(self, item: Optional[HttpCacheItem] = None):
        self.item = item
        self.__cache_generation = item.cache.generation if item else 0

    def not_modified(self) -> None:
        if self.item is not None:
            self.item.last_server_update = datetime.datetime.now()
            self.item.generation = self.__cache_generation

    @property
    def age(self) -> datetime.timedelta:
        if self.item is None:
            return datetime.timedelta.max
        return datetime.datetime.now() - self.item.last_server_update

    @property
    def might_have_been_modified(self) -> bool:
        return self.item.generation != self.__cache_generation

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class HttpCache:
    NOT_FOUND_RESPONSE = "404 Response"

    def __init__(self):
        self.__items: Dict[str, HttpCacheItem] = {}
        self.generation = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self):
        return len(self.__items)

    def __setitem__(self, key, value):
        self.__items.__setitem__(key, value)

    def __getitem__(self, item):
        self.__items.__getitem__(item)

    def close(self):
        self.__items.clear()
        self.__items = None

    def clear(self) -> None:
        self.__items.clear()

    def set(self, url: str, change_vector: str, result: str) -> None:
        http_cache_item = HttpCacheItem()
        http_cache_item.change_vector = change_vector
        http_cache_item.payload = result
        http_cache_item.cache = self
        http_cache_item.generation = self.generation
        self.__items[url] = http_cache_item

    def get(self, url: str) -> (ReleaseCacheItem, str, str):
        item = self.__items.get(url, None)
        if item is not None:
            change_vector = item.change_vector
            response = item.payload
            return ReleaseCacheItem(item), change_vector, response

        return ReleaseCacheItem(), None, None

    def set_not_found(self, url: str, aggressively_cached: bool) -> None:
        http_cache_item = HttpCacheItem()
        http_cache_item.change_vector = self.NOT_FOUND_RESPONSE
        http_cache_item.cache = self
        http_cache_item.generation = self.generation
        http_cache_item.flags = (
            {ItemFlags.AGGRESSIVELY_CACHED, ItemFlags.NOT_FOUND} if aggressively_cached else {ItemFlags.NOT_FOUND}
        )
        self.__items[url] = http_cache_item

    class ReleaseCacheItem:
        def __init__(self, item: HttpCacheItem = None):
            self.item: Union[None, HttpCacheItem] = item
            self.__cache_generation = item.cache.generation if item else 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def not_modified(self) -> None:
            if self.item is not None:
                self.item.last_server_update = datetime.datetime.now()
                self.item.generation = self.__cache_generation

        @property
        def age(self) -> datetime.timedelta:
            if self.item is None:
                return datetime.timedelta.max
            return datetime.datetime.now() - self.item.last_server_update

        @property
        def might_have_been_modified(self) -> bool:
            return self.item.generation != self.__cache_generation
