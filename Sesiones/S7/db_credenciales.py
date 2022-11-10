password = '12345'

import sqlalchemy

def conexion_repo_preparacion():
    username = 'root'
    hostname = 'localhost'
    database = 'repo_preparacion'
    port     = '3306'
    password = '12345'
    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(username, password,
                                                      hostname, database))
    return con

def conexion_repo_final():
    username = 'root'
    hostname = 'localhost'
    database = 'repo_preparacion'
    port     = '3306'
    password = '12345'
    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(username, password, 
                                                      hostname, database))
    return con


