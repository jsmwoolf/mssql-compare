import sqlparse
import logging
import datetime

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

class SQLCreateTable():
    """ https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver15 """
    def __init__(self, tokens):
        self.index = 2
        self.tokens = tokens
        self.tableName = ''
        self.columns = {}

        # Set up logger
        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.DEBUG)
        fh = logging.FileHandler(f'create-table-{datetime.datetime.now()}.log')
        fh.setLevel(level=logging.DEBUG)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.info(' '.join([token.value for token in self.tokens]))
        # Generate the metadata
        self._getMetaData()
        self.logger.removeHandler(fh)

    def __repr__(self):
        return str((f"CREATE TABLE {self.tableName}", self.getColumns()))

    def getColumns(self):
        return str(self.columns)

    def _nextToken(self):
        """ Allows you to go to the next token """
        self.index += 1
        self.logger.debug("Next token is {}".format(self.tokens[self.index]))
        return self.tokens[self.index]
    
    def _currentToken(self):
        self.logger.debug("Current token is {}".format(self.tokens[self.index]))
        return self.tokens[self.index]

    def _rewindToken(self):
        """ Allows you to rewind to the previous token """
        self.index -= 1
        self.logger.debug("Previous token is {}".format(self.tokens[self.index]))

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
            "ROWVERSION", "UNIQUEIDENTIFIER"
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

    def _getIdentityInfo(self):
        """ Parses the IDENTITY attributes """
        self.logger.debug("Begin _getIdentityInfo")
        info = None
        token = self._nextToken()
        if token.value == '(':
            seed = self._nextToken()
            self.logger.debug(seed.value)
            if not seed.value.isnumeric():
                raise Exception("Seed value is not numeric")
            token = self._nextToken()
            if not token.value == ',':
                raise Exception("Expected ,")
            increment = self._nextToken()
            if not increment.value.isnumeric():
                raise Exception("Increment value is not numeric")
            token = self._nextToken()
            if token.value != ')':
                raise Exception("Expected )")
            info = (int(seed.value), int(increment.value))
        else:
            token = self._rewindToken()
            info = (1, 1)
        self.logger.debug("End _getIdentityInfo")
        return info

    def _getDecimalInfo(self):
        self.logger.debug("Begin _getDecimalInfo")
        res = { 'precision': 8, 'scale': 0 }
        token = self._nextToken()
        if token.value == '(':
            precision = self._nextToken()
            if not precision.value.isnumeric():
                raise Exception("Precision value is not numeric")
            res['precision'] = int(precision.value)
            token = self._nextToken()
            # Check if there's a scale to evaluate
            if token.value == ')':
                return res
            elif not token.value == ',':
                raise Exception("Expected ,")
            
            scale = self._nextToken()
            if not scale.value.isnumeric():
                raise Exception("Scale value is not numeric")
            res['scale'] = int(scale.value)
            token = self._nextToken()
            if token.value != ')':
                raise Exception("Expected )")
        else:
            token = self._rewindToken()
        return res


    def _processDataType(self):
        """ Process the data type column """
        res = {}
        token = self._currentToken()
        res['data_type'] = token.value.upper()
        if token.value.upper() in { 'DECIMAL', 'NUMERIC' }:
            res |= self._getDecimalInfo()

        self._nextToken()
        return res

    def _parseColumn(self, column):
        self.logger.debug("Begin _parseColumn")
        self.columns[column] = {}
        token = self._nextToken()
        self.logger.debug(token.value)
        if self._isDataType(token.value):
            self.columns[column] = self._processDataType() 

            token = self._currentToken()
            while token.value not in { ',', ')' }:
                if token.value.upper() == 'IDENTITY':
                    self.columns[column]['identity'] = self._getIdentityInfo()
                token = self._nextToken()
            self._rewindToken() # If we hit a ',' or ')', go back one token
            self.logger.debug("End _parseColumn")
            return # Go to the next column or we reached the end of the table
        elif token.value.upper() == 'AS':
            pass
        pass

    def _parseColumns(self):
        self.logger.debug("Begin _parseColumns")
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
        self.logger.debug("End _parseColumns")

    def _getMetaData(self):
        tableName = self._currentToken()
        if type(tableName) == sqlparse.sql.Identifier:
            self.tableName = tableName.value
        else:
            raise Exception("Table name expected")
        token = self._nextToken() # Should be either ',' or AS
        if token.value.upper() == 'AS':
            pass
        if token.value == '(':
            self._parseColumns()
        else:
            raise Exception("Expected (")