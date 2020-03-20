import pyodbc
import pandas as pd
import sqlalchemy as db


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
engine = db.create_engine(connection_string)
connection_object = engine.connect()

# writing the query
query = """
    select
      estpermid as security_id
    ,	brokerid as broker_id
    ,	cast(perenddate as date) as period_date
    ,	cast(effectivedate as date) as estimate_date
    ,	defestvalue as value
    ,	b.description as currency
    , source_id = 'ibes'
    from
      tredetper a
    join
      trecode b
    on
      a.defcurrpermid = b.code
    and b.codetype = 7
    where
      measure = 9
    and pertype = 4
    and effectivedate >= getdate() - 365
"""

# executing a query
est_frame = pd.read_sql_query(query, connection_object)

est_frame.to_csv('estimate_raw.csv')

connection_object.close()