## Python Message Pack module

C library for python 3.10+.


### Installation
`pip install amsgpack`


### Example


```Python console
>>> from amsgpack import packb
>>> packb({"compact": True, "schema": 0})
bytearray(b'\x82\xa7compact\xc3\xa6schema\x00')
```

```Python console
>>> from amsgpack import Unpacker
>>> unpacker = Unpacker()
>>> unpacker.feed(b'\x82\xa7compact\xc3\xa6schema\x00')
>>> next(unpacker)
{'compact': True, 'schema': 0}
```
