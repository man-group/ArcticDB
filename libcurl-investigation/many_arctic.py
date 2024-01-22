from arcticdb import Arctic
import datetime
import time

def test():
    aas = []

    for i in range(1000):
        print(i, datetime.datetime.now())
        arctic = Arctic("s3://172.22.107.88:test?port=9000&access=pelAqd63Md4OFoTjiyDF&secret=mfoVw8MxqOr1HUImTkv3PLMgTYp0iGVprGAEMIf8")
        aas.append(arctic)
        arctic.has_library(f"lib_{i}")

test()
