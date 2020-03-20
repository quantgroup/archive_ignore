import sqlalchemy as db
import pyodbc

# user input requirements:
# * server name
# * database name
# * table name

def mk_connect_str(servname, dbname, dialect = 'mssql', driver = 'pyodbc', trust_connect = True, sql_driver = 'ODBC+Driver+17+for+SQL+Server'):
    """Creates a connection string for a server
    
    Arguments:
        servname {str} -- the full name of the server
        dbname {str} -- the full name of the database
    
    Keyword Arguments:
        dialect {str} -- sql dialect (default: {'myssql'})
        driver {str} -- the python driver (default: {'pyodbc'})
        trust_connect {bool} -- whether or not Windows Authenticate is used (default: {True})
        sql_driver {str} -- the provided database driver (default: {'ODBC+Driver+17+for+SQL+Server'})
    
    Raises:
        ValueError: does not support untrusted connections
    
    Returns:
        str -- a connection string that can be used to start a sqlalchemy engine
    """
    if trust_connect == True:
        connect_string = dialect + '+' + driver + '://' + servname + '/' + dbname + '?trusted_connection=yes&driver=' + sql_driver
    else:
        connect_string = None
        raise ValueError('This function does not support untrusted connections')
    return connect_string

# assigning the engine and creating the connection
connection_string = mk_connect_str('iamcqadpoc', 'qai')
print(connection_string)
engine = db.create_engine(connection_string)
connection_object = engine.connect()
metadata = db.MetaData()

# specifying the tables
estimate_tab = db.Table('tredetper', metadata, autoload = True, autoload_with=engine)

# writing the query
query = db.select([estimate_tab])

# executing the query
result_proxy = connection_object.execute(query)

# fetching the result
result_set = result_proxy.fetchone()

# displaying some output
print(result_set)

# closing the connection
connection_object.close()