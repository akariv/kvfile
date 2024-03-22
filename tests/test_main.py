import datetime
import decimal
import pytest
from kvfile.kvfile_leveldb import KVFileLevelDB, CachedKVFileLevelDB
from kvfile.kvfile_sqlite import KVFileSQLite, CachedKVFileSQLite
from kvfile.serializer import PickleSerializer, JsonSerializer

@pytest.mark.parametrize('KVFile', [KVFileLevelDB, KVFileSQLite])
@pytest.mark.parametrize('serializer', [PickleSerializer, JsonSerializer])
def test_sanity(KVFile, serializer):

    kv = KVFile(serializer())

    data = dict(
        s='value', 
        i=123, 
        d=datetime.datetime.fromtimestamp(12325), 
        n=decimal.Decimal('1234.56'),
        ss=set(range(10)),
        o=dict(d=decimal.Decimal('1234.58'), n=datetime.datetime.fromtimestamp(12325))
    )

    for k, v in data.items():
        kv.set(k, v)

    for k, v in data.items():
        assert kv.get(k) == v

    assert list(kv.keys()) == sorted(data.keys())
    assert list(kv.items()) == sorted(data.items())

    assert list(kv.keys(reverse=True)) == sorted(data.keys(), reverse=True)
    assert list(kv.items(reverse=True)) == sorted(data.items(), reverse=True)


@pytest.mark.parametrize('KVFile', [KVFileLevelDB, KVFileSQLite])
@pytest.mark.parametrize('serializer', [PickleSerializer, JsonSerializer])
def test_insert(KVFile, serializer):
    kv = KVFile(serializer())
    kv.insert(((str(i), ':{}'.format(i)) for i in range(50000)))
    assert len(list(kv.keys())) == 50000
    assert len(list(kv.items())) == 50000
    assert kv.get('49999') == ':49999'

    kv.insert(((str(i), ':{}'.format(i)) for i in range(50000, 100000)), batch_size=40000)
    assert len(list(kv.items())) == 100000

    kv.insert(((str(i), ':{}'.format(i)) for i in range(100000, 100002)), batch_size=1)
    kv.insert(((str(i), ':{}'.format(i)) for i in range(100002, 100005)), batch_size=0)
    assert len(list(kv.items())) == 100005


@pytest.mark.parametrize('KVFile', [KVFileLevelDB, KVFileSQLite])
@pytest.mark.parametrize('serializer', [PickleSerializer, JsonSerializer])
def test_insert_generator(KVFile, serializer):
    kv = KVFile(serializer())
    data = [(str(i), ':{}'.format(i)) for i in range(50)]
    expected_data = []
    for key, value in kv.insert_generator(data):
        expected_data.append((key, value))
    assert data == expected_data
    assert len(list(kv.keys())) == 50
    assert len(list(kv.items())) == 50
    assert kv.get('49') == ':49'


# Enable profiling using pytest_plugins = ['pytest_profiling']:

@pytest.mark.parametrize('KVFile', [KVFileLevelDB, KVFileSQLite, CachedKVFileLevelDB, CachedKVFileSQLite])
@pytest.mark.parametrize('serializer', [PickleSerializer, JsonSerializer])
@pytest.mark.parametrize('size', [100, 5000, 50000])
def test_cached(KVFile, serializer, size):
    from random import shuffle, randint, expovariate
    from math import floor
    kv = KVFile(serializer())
    s = size
    data = [
        ('%06d' % i, {'a': i}) for i in range(s)
    ]
    for k, v in data:
        kv.set(k, v)
    for i in range(3):
        shuffle(data)
        for j in range(s//2):
            j_ = randint(0, s-1)
            k, v = data[j_]
            v['a'] = randint(0, s)
            kv.set(k, v)
        shuffle(data)
        for j in range(s):
            j_ = floor(expovariate(0.1) * s)
            j_ = j_ % s
            k, v = data[j_]
            assert kv.get(k) == v
    items = list(kv.items())
    items = sorted(items)
    data = sorted(data)
    assert items == data
    keys = sorted(list(kv.keys()))
    assert keys == [x[0] for x in data]
    
@pytest.mark.parametrize(('KVFile', 'location'), [
    (KVFileLevelDB, 'test_temp/filename_leveldb_dummy'),
    (CachedKVFileLevelDB, 'test_temp/filename_c_leveldb_dummy'),
    (KVFileSQLite, 'test_temp/filename_sqlite_dummy/db'),
    (CachedKVFileSQLite, 'test_temp/filename_c_sqlite_dummy/db')
])
@pytest.mark.parametrize('serializer', [
    PickleSerializer,
    JsonSerializer
])
def test_filename(KVFile, location, serializer):
    import os
    os.makedirs(location, exist_ok=True)

    kv1 = KVFile(serializer(), location=location)
    kv1.insert(((str(i), ':{}'.format(i)) for i in range(50000)))
    kv1.close()

    kv = KVFile(serializer(), location=location)
    assert len(list(kv.keys())) == 50000
    assert len(list(kv.items())) == 50000
    assert kv.get('49999') == ':49999'
    kv.close()

@pytest.mark.parametrize('KVFile', [KVFileLevelDB, KVFileSQLite])
@pytest.mark.parametrize('serializer', [PickleSerializer, JsonSerializer])
def test_default(KVFile, serializer):
    kv = KVFile(serializer())
    kv.set('aaaa', 5)
    assert kv.get('aaaa') == 5
    assert kv.get('bbbb', default=6) == 6
    with pytest.raises(KeyError):
        kv.get('bbbb')
        