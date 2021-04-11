import sqlparse
import logging
import datetime

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

mode = logging.DEBUG
class SQLCreateTable():
    """ https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver15 """
    def __init__(self, tokens):
        self.index = 2
        self.tokens = tokens
        self.tableName = ''
        self.columns = {}
        self.multiForeignKeys = []

        # Set up logger
        self.logger = logging.getLogger()
        self.logger.setLevel(level=mode)
        fh = logging.FileHandler(f'logs/create-table-{datetime.datetime.now()}.log')
        fh.setLevel(level=mode)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.info(' '.join([token.value for token in self.tokens]))
        # Generate the metadata
        self._getMetaData()
        fh.close()
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

    def _isPunctuation(self, token):
        grammar = {
            ',', "'", '"', ';',
            '$', '@', '+', '-',
            '_', '=', '(', ')',
            '{', '[', ']', '}',
            ':', '<', '>', '/'
        }
        return token in grammar

    def _isKeyword(self, token):
        keywords = {
            # Constraint Keys
            'PRIMARY', 'FOREIGN', 'KEY', 'REFERENCES',
            # DDL
            'CREATE', 'DROP', 'ALTER',
            'TABLE', 'IDENTITY', 'NOT', 'NULL', 'ON',
            'SPARSE', 'CLUSTERED', 'NONCLUSTERED',
            'DEFAULT', 'ADD',
            # SQL DML
            'DELETE', 'UPDATE', 'INSERT', 'SELECT'
        }
        return token.upper() in keywords

    def _isIdentifier(self, token):
        return not (self._isKeyword(token) or self._isDataType(token) or self._isPunctuation(token))

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

    def _getForiegnKey(self, tableMode=False):
        """ Parses the PRIMARY KEY attributes """
        self.logger.debug("Begin _getForiegnKey")
        res = {}
        token = self._nextToken()
        if token.value.upper() != 'KEY':
            raise Exception("Expected keyword KEY")

        # For table constraint, get mulitple columns
        if tableMode:
            token = self._nextToken()
            if token.value != '(':
                raise Exception("Expected (")
            res['columns'] = []
            while True:
                token = self._nextToken()
                if not self._isIdentifier(token.value):
                    raise Exception("Expected identifier")
                res['columns'].append(token.value)
                token = self._nextToken()
                if token.value == ')':
                    break
                elif token.value != ',':
                    raise Exception("Expected ,")

        token = self._nextToken()
        if token.value.upper() != 'REFERENCES':
            raise Exception("Expected keyword REFERENCES")

        token = self._nextToken()
        if not self._isIdentifier(token.value):
            raise Exception("Expected identifier")
        res['ref_table'] = token.value

        token = self._nextToken()
        if token.value != '(':
            raise Exception("Expected (")
        
        if tableMode:
            res['ref_column'] = []
            while True:
                token = self._nextToken()
                if not self._isIdentifier(token.value):
                    raise Exception("Expected identifier")
                res['ref_column'].append(token.value)
                token = self._nextToken()
                if token.value == ')':
                    self._rewindToken()
                    break
                elif token.value != ',':
                    raise Exception("Expected ,")
        else:
            token = self._nextToken()
            if not self._isIdentifier(token.value):
                raise Exception("Expected identifier")
            res['ref_column'] = token.value

        token = self._nextToken()
        if token.value != ')':
            raise Exception("Expected )")
        
        token = self._nextToken()
        if token.value.upper() == 'ON':
            while True:
                token = self._nextToken()
                print(token)
                if token.value.upper() not in {'DELETE', 'UPDATE'}:
                    break
                action = token.value
                token = self._nextToken()
                if token.value.upper() == 'CASCADE':
                    res[action] = 'CASCADE'
                elif token.value.upper() =='SET':
                    token = self._nextToken()
                    res[action] = token.value.upper()
                elif token.value.upper() == 'NO':
                    self._nextToken()
                    res[action] = 'NONE'
            self._rewindToken()
        elif token.value.upper() == 'NOT':
            pass
        else:
            self._rewindToken()
        self.logger.debug("End _getForiegnKey")
        return res

    def _getPrimaryKeyColumn(self):
        """ Parses the PRIMARY KEY attributes """
        res = {}
        token = self._nextToken()
        if token.value.upper() != 'KEY':
            raise Exception("Expected keyword KEY")
        res['primary_key'] = True
        token = self._nextToken()

        # Check for clustered indexing
        if token.value.upper() == 'NONCLUSTERED':
            res['clustered'] = False
            token = self._nextToken()
        elif token.value.upper() == 'CLUSTERED':
            res['clustered'] = True
            token = self._nextToken()

        token = self._rewindToken()
        return res

    def _getPrimaryKeyTable(self):
        """ Parses the PRIMARY KEY attributes """
        res = {}
        column = ''
        token = self._nextToken()
        if token.value.upper() != 'KEY':
            raise Exception("Expected keyword KEY")
        res['primary_key'] = True
        token = self._nextToken()

        # Check for clustered indexing
        if token.value.upper() == 'NONCLUSTERED':
            res['clustered'] = False
            token = self._nextToken()
        elif token.value.upper() == 'CLUSTERED':
            res['clustered'] = True
            token = self._nextToken()
        
        if token.value == '(':
            token = self._nextToken()
            if token.value not in self.columns:
                raise Exception(f"The column {token.value} doesn't exist")
            column = token.value
            token = self._nextToken()
            if token.value != ')':
                raise Exception(f"Expected )")
            token = self._nextToken()
        else:
            raise Exception(f"Expected (")
        
        token = self._rewindToken()
        self.columns[column] |= res

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
        """ Parses the DECIMAL/NUMERIC attributes """
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
        self.logger.debug("End _getDecimalInfo")
        return res

    def _getDataSize(self, dtype):
        """ Parses the FLOAT, VARCHAR, NCHAR, NVARCHAR, CHAR, BINARY, VARBINARY attributes """
        self.logger.debug("Begin _getDataSize")
        res = { 'size': 1 }
        token = self._nextToken()
        if token.value == '(':
            size = self._nextToken()
            if not size.value.isnumeric():
                raise Exception("Precision value is not numeric")
            res['size'] = int(size.value)
            token = self._nextToken()
            # Check if there's a scale to evaluate
            if not token.value == ')':
                raise Exception("Expected ,")
        else:
            if dtype == 'FLOAT':
                res['size'] = 53
            token = self._rewindToken()
        return res


    def _processDataType(self):
        """ Process the data type column """
        res = {}
        token = self._currentToken()
        res['data_type'] = token.value.upper()
        if token.value.upper() in { 'DECIMAL', 'NUMERIC' }:
            res |= self._getDecimalInfo()
        elif token.value.upper() in { 'FLOAT', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'CHAR', 'BINARY', 'VARBINARY' }:
            res |= self._getDataSize(token.value.upper())
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
                #elif token.value.upper() == 'NOT':
                #    token = self._nextToken()
                #    if token.value.upper() == 'NULL':
                #        self.columns[column]['nullable'] = False
                elif token.value.upper() == 'NOT NULL':
                    self.columns[column]['nullable'] = False
                elif token.value.upper() == 'NULL':
                    self.columns[column]['nullable'] = True
                elif token.value.upper() == 'DEFAULT':
                    token = self._nextToken()
                    self.columns[column]['default_value'] = token.value
                elif token.value.upper() == 'PRIMARY':
                    self.columns[column] |= self._getPrimaryKeyColumn()
                elif token.value.upper() == 'FOREIGN':
                    self.columns[column]['foreign_key'] = self._getForiegnKeyColumn()

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
            #print(f'Next token for column: {token}')
            #print(self.columns)
            #self.logger.debug("Look for Identifier: ", token, type(token), type(token) == sqlparse.sql.Identifier)
            if self._isIdentifier(token.value.upper()):
                self._parseColumn(token.value)
            elif token.value.upper() == 'PRIMARY':
                self._getPrimaryKeyTable()
            elif token.value.upper() == 'FOREIGN':
                obj = self._getForiegnKey(tableMode=True)
                if len(obj['columns']) == 1:
                    obj['ref_column'] = obj['ref_column'][0]
                    column = obj['columns'][0]
                    del obj['columns']
                    self.columns[column]['foreign_key'] = obj
                else:
                    self.multiForeignKeys.append(obj)
            elif token.value == ',':
                continue
            elif token.value == ')':
                break
        self.logger.debug("End _parseColumns")

    def _getMetaData(self):
        tableName = self._currentToken()
        if self._isIdentifier(tableName.value.upper()):
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