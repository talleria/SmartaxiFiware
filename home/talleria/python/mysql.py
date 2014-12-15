import MySQLdb as mdb
import datos as d
import sys

connection = None

def getListData(query):
    global connection
    
    try:
        if connection == None:
            connection = mdb.connect(d.MYSQL_HOST, d.MYSQL_USER, d.MYSQL_PASS, d.MYSQL_DB);
        cur = connection.cursor()
        cur.execute(query)
        row = cur.fetchall()
        
    except mdb.Error, e:
        sys.stderr.writelines("Error %d: %s\n" %(e.args[0],e.args[1]))
        sys.stderr.writelines("SQL: %s\n" %(query))
        sys.exit(1)
    
    return row

def execute(query):
    global connection
    
    try:
        if connection == None:
            connection = mdb.connect(d.MYSQL_HOST, d.MYSQL_USER, d.MYSQL_PASS, d.MYSQL_DB);
        cur = connection.cursor()
        cur.execute(query)
        connection.commit()
        
    except mdb.Error, e:
        sys.stderr.writelines("Error %d: %s \n" % (e.args[0],e.args[1]))
        sys.stderr.writelines("SQL: %s\n" %(query))
        sys.exit(1)

def closeConnection():
    global connection
    
    if connection != None:
        connection.close()
        connection = None
        