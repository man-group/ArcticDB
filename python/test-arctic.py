from arcticdb import Arctic
import pandas as pd


ac = Arctic('lmdb://test-test')
lib = ac.get_library('test', create_if_missing=True)

df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

lib.write('test', df)
print(lib.read('test').data)
