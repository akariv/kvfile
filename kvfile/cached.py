from typing import Iterator
import cachetools

from .serializer_base import SerializerBase
from .base import KVFileBase

class DBWriteOnEvictionLRUCache(cachetools.LRUCache):

    def __init__(self, access_db, dirty_set, *args, **kw):
        super().__init__(*args, **kw)
        self.access_db = access_db
        self.dirty_set = dirty_set

    def popitem(self):
        key, value = super().popitem()
        # print('popitem', key, value)
        if key in self.dirty_set:
            self.access_db()._set_db(key, value)
            self.dirty_set.discard(key)
        return key, value


class CachedKVFile(KVFileBase):

    DEFAULT_CACHE_SIZE = 10240

    def __init__(self, kvfile_cls: KVFileBase=None, serializer: SerializerBase=None, filename=None, size=DEFAULT_CACHE_SIZE):
        super().__init__(serializer, filename)
        self.dirty = set()
        self.cache = DBWriteOnEvictionLRUCache(self.db, self.dirty, size)
        self.kvfile_cls = kvfile_cls
        self._db = None

    def db(self):
        if self._db is None:
            self._db = self.kvfile_cls(self.serializer, self.filename)
        return self._db

    def _get_db(self, key: str) -> bytes:
        if key in self.cache:
            return self.cache[key]
        else:
            ret = self.db()._get_db(key)
            if ret is not None:
                self.cache[key] = ret
                self.dirty.discard(key)
            return ret

    def _set_db(self, key: str, value: bytes) -> None:
        self.cache[key] = value
        self.dirty.add(key)

    def _del_db(self, key: str) -> None:
        self.cache.pop(key, None)
        if self._db is not None:
            self.db()._del_db(key)
        self.dirty.discard(key)

    def keys(self, reverse=False) -> Iterator[str]:
        if self._db is not None:
            self.flush()
            return self.db().keys()
        return sorted(self.cache.keys(), reverse=reverse)

    def items(self, reverse=False):
        if self._db is not None:
            self.flush()
            yield from self.db().items(reverse)
        else:
            for key in self.keys(reverse):
                yield key, self.get(key)

    def flush(self):
        self.db()._set_db_batch(
            (k, v)
            for k, v in self.cache.items()
            if k in self.dirty
        )
        self.dirty.clear()