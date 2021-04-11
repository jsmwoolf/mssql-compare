import unittest
from src.parser.parser import convertToMetaData

class TestCreateTableBasic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Starting up {cls.__name__}")
    
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

class TestCreateTableDataType(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Starting up {cls.__name__}")

    def setUp(self):
        print(f"Running test: {self._testMethodName}")
    
    def test_numeric(self):
        query = "CREATE TABLE test1 (testid NUMERIC);"
        meta = convertToMetaData(query)
        print(meta)

    def test_numeric_precision(self):
        query = "CREATE TABLE test1 (testid NUMERIC(3));"
        meta = convertToMetaData(query)
        print(meta)

    def test_numeric_full(self):
        query = "CREATE TABLE test1 (testid NUMERIC(3, 4));"
        meta = convertToMetaData(query)
        print(meta)

    def test_decimal(self):
        query = "CREATE TABLE test1 (testid DECIMAL);"
        meta = convertToMetaData(query)
        print(meta)

    def test_decimal_precision(self):
        query = "CREATE TABLE test1 (testid DECIMAL(3));"
        meta = convertToMetaData(query)
        print(meta)

    def test_decimal_full(self):
        query = "CREATE TABLE test1 (testid DECIMAL(3, 4));"
        meta = convertToMetaData(query)
        print(meta)


class TestCreateTableIdentity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Starting up {cls.__name__}")
    
    def setUp(self):
        print(f"Running test: {self._testMethodName}")

    def test_identity_minimum(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY);"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_full(self):
        query = "CREATE TABLE test1 (test1a BIGINT IDENTITY(1,2));"
        meta = convertToMetaData(query)
        print(meta)

    def test_identity_incomplete(self):
        with self.assertRaises(Exception) as context:   
            query = "CREATE TABLE test1 (test1a BIGINT IDENTITY(1));"
            convertToMetaData(query)
            self.assertTrue('Expected ,' in context.exception)

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