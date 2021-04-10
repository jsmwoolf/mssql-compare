import unittest
from src.parser.parser import convertToMetaData

class TestCreateTable(unittest.TestCase):
    def setUp(self):
        print(f"Running test: {self._testMethodName}")
    
    def test_one_column(self):
        query = "CREATE TABLE test1 (testid INT);"
        meta = convertToMetaData(query)
        print(meta)

    def test_multi_column(self):
        query = "CREATE TABLE test2 ( test2id INT, test2c BIGINT, test2b SMALLINT );"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_minimum(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY);"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_full(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY(1,2));"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_incomplete(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY(1));"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_minimum_comma(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY, testid INT);"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_full_comma(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY(1,2), testid INT);"
        meta = convertToMetaData(query)
        print(meta)


if __name__ == '__main__':
    unittest.main()