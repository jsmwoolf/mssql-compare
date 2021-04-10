import sqlparse
from .tables.create import SQLCreateTable

def expandTokens(tokens):
    res = []
    for token in tokens:
        if type(token) == sqlparse.sql.Comment or token.value[:2] == '--':
            continue
        elif type(token) in [ sqlparse.sql.IdentifierList, sqlparse.sql.Function, sqlparse.sql.Identifier, sqlparse.sql.Parenthesis ] :
            if type(token) == sqlparse.sql.Identifier and len(token.tokens) == 1:
                res.append(token)
            else:
                res += expandTokens(token.tokens)
            continue
        elif token.value == ' ':
            continue
        elif token.value == '\n':
            continue
        else:
            res.append(token)
    return res

def convertToMetaData(sqlCode):
    statements = sqlparse.parse(sqlCode)
    
    stmtCount = 0
    metaDatas = []
    for statement in statements:
        tokens = expandTokens(statement.tokens)
        #print(tokens)
        if len(tokens) == 0:
            continue
        stmtCount += 1
        firstToken = tokens[0]
        if firstToken.value.upper() == 'ALTER':
            pass
        elif firstToken.value.upper() == 'CREATE':
            objectType = tokens[1]
            if objectType.value.upper() == 'TABLE':
                metaDatas.append((SQLCreateTable(tokens), stmtCount-1)) 
        
    return metaDatas

def convertFileToMetaData(filename):
    with open(filename, 'r') as f:
        sqlCode = f.read()
        return convertToMetaData(sqlCode)
