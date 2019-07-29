from itertools import chain
import cachetools
from .kvfile import KVFile


class CachedKVFile(cachetools.LRUCache):

    def __init__(self, size=1024):
        super().__init__(size)
        self.db = KVFile()

    def get(self, key):
        return self[key]

    def set(self, key, value):
        self[key] = value

    def insert(self, key_value_iterator, batch_size=1000):
        for key, value in key_value_iterator:
            self.set(key, value)

    def keys(self, reverse=False):
        return chain(iter(self), self.db.keys())

    def items(self, reverse=False):
        return chain(((k, self[k]) for k in iter(self)), self.db.items(reverse))

    def popitem(self):
        key, value = super().popitem()
        self.db.set(key, value)
        return key, value

    def __missing__(self, key):
        value = self.db.get(key)
        self.db.delete(key)
        self[key] = value
        return value
