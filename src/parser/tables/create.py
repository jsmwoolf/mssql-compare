import sqlparse
import logging
import datetime

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

class SQLCreateTable():
    """ https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver15 """
    def __init__(self, tokens):
        self.metaData = {'name': None, 'columns': {}, 'depends_on': {}}
        self.index = 2
        self.tokens = tokens
        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.INFO)
        fh = logging.FileHandler(f'create-table-{datetime.datetime.now()}.log')
        fh.setLevel(level=logging.INFO)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.info(' '.join([token.value for token in self.tokens]))
        # Generate the metadata
        self._getMetaData()
        self.logger.removeHandler(fh)
    
    def __repr__(self):
        return str(self.metaData)

    def _nextToken(self):
        """ Allows you to go to the next token """
        self.index += 1
        return self.tokens[self.index]
    
    def _currentToken(self):
        return self.tokens[self.index]

    def _rewindToken(self):
        """ Allows you to rewind to the previous token """
        self.index -= 1

    def _isDataType(self, token):
        """ https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15 """
        dtypes = {
            # Exact Numerics
            "BIGINT", "NUMERIC", "BIT", "SMALLINT", "DECIMAL", "SMALLMONEY", "INT", "TINYINT", "MONEY",
            # Approximate Numerics
            "FLOAT", "REAL",
            # Date and Time
            "DATE", "DATETIMEOFFSET", "DATETIME2", "SMALLDATETIME", "DATETIME", "TIME",
            # Character Strings
            "CHAR", "VARCHAR", "TEXT",
            # Unicode Character strings
            "NCHAR", "NVARCHAR", "NTEXT",
            # Binary Strings
            "BINARY", "VARBINARY", "IMAGE",
            # Other
            "ROWVERSION", "UNIQUEIDENTIFIER", ""
        }
        return token.upper() in dtypes

    def _isKeyword(self, token):
        keywords = ['PRIMARY', 'FOREIGN', 'KEY', 'REFERENCES', 'CREATE', 'TABLE', 'IDENTITY', 'NOT', 'NULL', 'ON']
        return token.upper() in keywords or self._isDataType(token)

    def _getNumberSizes(self, token):
        return [int(tkn.value) for tkn in self._filterTokens(token.tokens) if tkn.value.isnumeric()]

    def _getKeys(self, tokens):
        res = []
        for token in tokens:
            if type(token) == sqlparse.sql.Identifier:
                res.append(token.value.upper())
        return tuple(res)

    def _validForeignReference(self, data):
        return data['table_reference'] in self.metaData or data['delete_action'] != 'NO_ACTION'

    def _getPrimaryKey(self, tokens, index):
        if tokens[index].value.upper() != 'KEY':
            raise Exception("Expected keyword KEY")
        index += 1
        if type(tokens[index]) == sqlparse.sql.Parenthesis:
            return (index+1, self._getKeys(tokens[index].tokens))
        return (index, self.inColumn)

    def _getForiegnKey(self, tokens, index):
        if tokens[index].value.upper() != 'KEY':
            raise Exception("Expected keyword KEY")
        index += 1
        selfRef = None
        if type(tokens[index]) == sqlparse.sql.Parenthesis:
            selfRef = self._getKeys(tokens[index].tokens)
            index += 1
        else:
            selfRef = self.inColumn
        if tokens[index].value.upper() != 'REFERENCES':
            raise Exception("Expected keyword REFERENCES")
        index += 1
        if type(tokens[index]) == sqlparse.sql.Parenthesis:
            raise Exception("Expected table name")
        tableReference = tokens[index].value.upper()
        self.dependents.append(tableReference)
        index += 1
        if type(tokens[index]) != sqlparse.sql.Parenthesis:
            raise Exception("Expected column references")
        foreignKeys = self._getKeys(tokens[index].tokens)
        index += 1
        special = {
            'DELETE': 'NO_ACTION',
            'UPDATE': 'NO_ACTION'
        }
        if tokens[index] != 'ON':
            pass
        return (index, selfRef, tableReference, foreignKeys, special)

    def _getIdentityAttributes(self):
        """ Parses the IDENTITY attributes """
        token = self._nextToken()
        self.logger.debug(token.value)
        if token.value == '(':
            seed = self._nextToken()
            self.logger.debug(seed.value)
            if not seed.value.isnumeric():
                raise Exception("Seed value is not numeric")
            token = self._nextToken()
            self.logger.debug(token.value)
            if not token.value == ',':
                raise Exception("Expected ,")
            increment = self._nextToken()
            self.logger.debug(increment.value)
            if not increment.value.isnumeric():
                raise Exception("Increment value is not numeric")
            token = self._nextToken()
            if token.value != ')':
                raise Exception("Expected )")
            self._nextToken()
            return (int(seed.value), int(increment.value))
        else:
            return (1, 1)

    def _parseColumn(self, column):
        self.metaData['columns'][column] = {}
        token = self._nextToken()
        self.logger.debug(token.value)
        if self._isDataType(token.value):
            self.metaData['columns'][column]['data_type'] = token.value.upper()
            while token.value not in { ',', ')' }:
                token = self._nextToken()
                if token.value.upper() == 'IDENTITY':
                    self.metaData['columns'][column]['identity'] = self._getIdentityAttributes()
                token = self._currentToken()
            self._rewindToken() # If we hit a ',' or ')', go back one token
            return # Go to the next column or we reached the end of the table
        pass

    def _parseColumns(self):
        while True:
            token = self._nextToken() # Should be either identitiy token, ',', or ')'
            #self.logger.debug("Look for Identifier: ", token, type(token), type(token) == sqlparse.sql.Identifier)
            if type(token) == sqlparse.sql.Identifier or token.value.upper() == 'ID':
                if token.value.upper() == 'ID':
                    self.logger.info("We don't recommend ID for a column name.") 
                self._parseColumn(token.value)
            elif token.value == ',':
                continue
            elif token.value == ')':
                break
            #if token 

    def _getMetaData(self):
        tableName = self._currentToken()
        if type(tableName) == sqlparse.sql.Identifier:
            self.metaData['name'] = tableName.value.upper()
        else:
            raise Exception("Table name expected")
        token = self._nextToken() # Should be either ',' or AS
        if token.value.upper() == 'AS':
            pass
        if token.value == '(':
            self._parseColumns()
        else:
            raise Exception("Expected (")