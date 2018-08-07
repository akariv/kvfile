import tempfile

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

    def keys(self, reverse=False):
        for key, value in self.db.iterator(reverse=reverse):
            yield key.decode('utf8')

    def items(self, reverse=False):
        for key, value in self.db.iterator(reverse=reverse):
            yield (key.decode('utf8'),
                   self.serializer.deserialize(value.decode('utf8')))


KVFile = LevelDB if db_kind == 'LevelDB' else SqliteDB
