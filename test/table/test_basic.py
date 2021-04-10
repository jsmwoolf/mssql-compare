import unittest
from src.parser.parser import convertToMetaData

class TestTableBasic(unittest.TestCase):
    def test_one_column(self):
        query = "CREATE TABLE test1 (testid INT);"
        meta = convertToMetaData(query)
        print(meta)

    def test_one(self):
        query = "CREATE TABLE test2 ( test2id INT, test2c BIGINT, test2b SMALLINT );"
        meta = convertToMetaData(query)
        print(meta)

if __name__ == '__main__':
    unittest.main()