# kvfile

[![Travis](https://img.shields.io/travis/akariv/kvfile/master.svg)](https://travis-ci.org/akariv/kvfile)
[![Coveralls](http://img.shields.io/coveralls/akariv/kvfile.svg?branch=master)](https://coveralls.io/r/akariv/kvfile?branch=master)

A simple Key-Value store that's file based - so can accommodate large data sets with a small memory footprint.

Internally will use the faster `leveldb` as a storage backend or `sqlite3` as fallback if `leveldb` is not available.

## The Basics

The API should feel familiar to anyone working with Python.
It exposes `get`, `keys` and `items` for reading from the DB, and `set` for setting a value in the DB.

### Initializing

```python
import datetime
import decimal

from kvfile import KVFile

kv = KVFile()
```

### Setting values

```python
kv.set('s', 'value')
kv.set('i', 123)
kv.set('d', datetime.datetime.fromtimestamp(12325))
kv.set('n', decimal.Decimal('1234.56'))
kv.set('ss', set(range(10)))
kv.set('o', dict(d=decimal.Decimal('1234.58'), 
                 n=datetime.datetime.fromtimestamp(12325)))
```

### Getting values

```python
assert kv.get('s') == 'value'
assert kv.get('i') == 123
assert kv.get('d') == datetime.datetime.fromtimestamp(12325)
assert kv.get('n') == decimal.Decimal('1234.56')
assert kv.get('ss') == set(range(10))
assert kv.get('o') == dict(d=decimal.Decimal('1234.58'), 
                           n=datetime.datetime.fromtimestamp(12325))
```

### Listing values

`keys()` and `items()` methods return a generator yielding the values for efficient stream processing.

The returned data is sorted ascending (by default) based on the keys

```python
assert list(kv.keys()) == ['d', 'i', 'n', 'o', 's', 'ss']
assert list(kv.items()) == [
  ('d', datetime.datetime.fromtimestamp(12325)), 
  ('i', 123), 
  ('n', decimal.Decimal('1234.56')), 
  ('o', {'d': decimal.Decimal('1234.58'), 
         'n': datetime.datetime.fromtimestamp(12325)}), 
  ('s', 'value'), 
  ('ss', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
]
```

Set the `reverse` argument to True for the `keys()` and `items()` methods to sort in descending order.

### Bulk inserting data

The SQLite DB backend can be very slow when bulk inserting data. You can use the insert method to insert efficiently in bulk.

```python
kv.insert(((str(i), ':{}'.format(i)) for i in range(50000)))
```

The batch size is 1000 by default, you should modify it depending on the size of your data and available memory.

```python
kv.insert(((str(i), ':{}'.format(i)) for i in range(50000)), batch_size=40000)
```

If you are inserting data from a generator and need to use the inserted data, use `insert_generator` method:

```python
for key, value in kv.insert_generator(((str(i), ':{}'.format(i)) for i in range(50)), batch_size=10):
    print(key, value)
```

## Installing leveldb

On Debian based Linux:
```bash
$ apt-get install libleveldb-dev libleveldb1
```

On Alpine based Linux:
```bash
$ apk --repository http://dl-3.alpinelinux.org/alpine/edge/testing/ --update add leveldb leveldb-dev
```

On OS X:
```bash
$ brew install leveldb
```

