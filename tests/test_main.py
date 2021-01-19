import datetime
import decimal
import pytest

def test_sanity():
    from kvfile import KVFile

    kv = KVFile()

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

def test_insert():
    from kvfile import KVFile
    kv = KVFile()
    kv.insert(((str(i), ':{}'.format(i)) for i in range(50000)))
    assert len(list(kv.keys())) == 50000
    assert len(list(kv.items())) == 50000
    assert kv.get('49999') == ':49999'

    kv.insert(((str(i), ':{}'.format(i)) for i in range(50000, 100000)), batch_size=40000)
    assert len(list(kv.items())) == 100000

    kv.insert(((str(i), ':{}'.format(i)) for i in range(100000, 100002)), batch_size=1)
    kv.insert(((str(i), ':{}'.format(i)) for i in range(100002, 100005)), batch_size=0)
    assert len(list(kv.items())) == 100005

def test_insert_generator():
    from kvfile import KVFile
    kv = KVFile()
    data = [(str(i), ':{}'.format(i)) for i in range(50)]
    expected_data = []
    for key, value in kv.insert_generator(data):
        expected_data.append((key, value))
    assert data == expected_data
    assert len(list(kv.keys())) == 50
    assert len(list(kv.items())) == 50
    assert kv.get('49') == ':49'

def test_cached():
    from kvfile import CachedKVFile
    from random import shuffle
    kv = CachedKVFile()
    s = 5000
    data = [
        ('%06d' % i, {'a': i}) for i in range(s)
    ]
    for k, v in data:
        kv.set(k, v)
    for i in range(3):
        shuffle(data)
        for k, v in data[:(s//2)]:
            kv.set(k, v)
        shuffle(data)
        for k, v in data[:(s//2)]:
            assert kv.get(k) == v
    items = list(kv.items())
    items = sorted(items)
    data = sorted(data)
    assert items == data
    keys = sorted(list(kv.keys()))
    assert keys == [x[0] for x in data]
    
def test_filename():
    from kvfile import KVFile, db_kind
    filename = 'bla.filename.' + db_kind + '.db'
    kv1 = KVFile(filename=filename)
    kv1.insert(((str(i), ':{}'.format(i)) for i in range(50000)))
    del kv1

    kv = KVFile(filename=filename)
    assert len(list(kv.keys())) == 50000
    assert len(list(kv.items())) == 50000
    assert kv.get('49999') == ':49999'

def test_default():
    from kvfile import KVFile
    kv = KVFile()
    kv.set('aaaa', 5)
    assert kv.get('aaaa') == 5
    assert kv.get('bbbb', default=6) == 6
    with pytest.raises(KeyError):
        kv.get('bbbb')
        