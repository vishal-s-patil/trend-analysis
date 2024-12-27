import vertica_python
import pandas as pd 

def create_connection(host, username, password, database, port=5433, autoCommit=False):
    connection_config = {
        'host': host,
        'port': port,
        'user': username,
        'password': password,
        'database': database,
        'autocommit': autoCommit
    }
    
    connection = vertica_python.connect(**connection_config)
    return connection

def read(connection, query, columns):
    '''
    reads specified columns to a dataframe 
    '''
    cursor = connection.cursor()
    cursor.execute(query)

    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    
    cursor.close()
    return df