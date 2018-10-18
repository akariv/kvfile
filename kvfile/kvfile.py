import tempfile
from collections import deque

try:
    import plyvel as DB_ENGINE
    db_kind = 'LevelDB'
except ImportError:
    import sqlite3 as DB_ENGINE
    db_kind = 'sqlite'

from .serializer import JsonSerializer


class SqliteDB(object):

    def __init__(self, serializer=JsonSerializer):
        self.tmpfile = tempfile.NamedTemporaryFile()
        self.serializer = serializer()
        self.db = DB_ENGINE.connect(self.tmpfile.name)
        self.cursor = self.db.cursor()
        self.cursor.execute('''CREATE TABLE d (key text, value text)''')
        self.cursor.execute('''CREATE UNIQUE INDEX i ON d (key)''')

    def get(self, key):
        ret = self.cursor.execute('''SELECT value FROM d WHERE key=?''',
                                  (key,)).fetchone()
        if ret is None:
            raise KeyError()
        else:
            return self.serializer.deserialize(ret[0])

    def set(self, key, value):
        value = self.serializer.serialize(value)
        try:
            self.get(key)
            self.cursor.execute('''UPDATE d SET value=? WHERE key=?''',
                                (value, key))
        except KeyError:
            self.cursor.execute('''INSERT INTO d VALUES (?, ?)''',
                                (key, value))
        self.db.commit()

    def insert(self, key_value_iterator, batch_size=1000):
        deque(self.insert_generator(key_value_iterator, batch_size))

    def insert_generator(self, key_value_iterator, batch_size=1000):
        if batch_size == 1:
            for key, value in key_value_iterator:
                yield key, value
                self.set(key, value)
        else:
            batch = []

            def flush(force=False):
                if len(batch) > 0 and (force or (batch_size and len(batch) >= batch_size)):
                    self.cursor.executemany('''INSERT INTO d VALUES (?, ?)''', batch)
                    self.db.commit()
                    batch.clear()
            for key, value in key_value_iterator:
                yield key, value
                value = self.serializer.serialize(value).encode()
                batch.append((key, value))
                flush()
            flush(force=True)

    def keys(self, reverse=False):
        cursor = self.db.cursor()
        direction = 'DESC' if reverse else 'ASC'
        keys = cursor.execute('''SELECT key FROM d ORDER BY key ''' + direction)
        for key, in keys:
            yield key

    def items(self, reverse=False):
        cursor = self.db.cursor()
        direction = 'DESC' if reverse else 'ASC'
        items = cursor.execute('''SELECT key, value FROM d ORDER BY key ''' + direction)
        for key, value in items:
            yield key, self.serializer.deserialize(value)


class LevelDB(object):

    def __init__(self, serializer=JsonSerializer):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.serializer = serializer()
        self.db = DB_ENGINE.DB(self.tmpdir.name, create_if_missing=True)

    def get(self, key):
        ret = self.db.get(key.encode('utf8'))
        if ret is None:
            raise KeyError()
        else:
            return self.serializer.deserialize(ret.decode('utf8'))

    def set(self, key, value):
        value = self.serializer.serialize(value).encode('utf8')
        key = key.encode('utf8')
        self.db.put(key, value)

    def insert(self, key_value_iterator, batch_size=1000):
        deque(self.insert_generator(key_value_iterator, batch_size))

    def insert_generator(self, key_value_iterator, batch_size=1000):
        if batch_size == 1:
            for key, value in key_value_iterator:
                yield key, value
                self.set(key, value)
        else:
            batch = []

            def flush(force=False):
                if len(batch) > 0 and (force or (batch_size and len(batch) >= batch_size)):
                    write_batch = self.db.write_batch()
                    for key, value in batch:
                        write_batch.put(key, value)
                    write_batch.write()
                    write_batch.clear()
                    del write_batch
                    batch.clear()
            for key, value in key_value_iterator:
                yield key, value
                value = self.serializer.serialize(value).encode('utf8')
                key = key.encode('utf8')
                batch.append((key, value))
                flush()
            flush(True)

    def keys(self, reverse=False):
        for key, value in self.db.iterator(reverse=reverse):
            yield key.decode('utf8')

    def items(self, reverse=False):
        for key, value in self.db.iterator(reverse=reverse):
            yield (key.decode('utf8'),
                   self.serializer.deserialize(value.decode('utf8')))


KVFile = LevelDB if db_kind == 'LevelDB' else SqliteDB
