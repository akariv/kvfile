import datetime
import decimal


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
