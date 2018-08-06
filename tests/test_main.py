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
