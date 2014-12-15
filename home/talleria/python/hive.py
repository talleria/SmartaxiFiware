

import sys
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
sys.path.insert(0, '/home/smartaxi/python/py/')

def getDataHie(query):
	try:
	    transport = TSocket.TSocket('cosmos.lab.fi-ware.org', 10000)
	    transport = TTransport.TBufferedTransport(transport)
	    protocol = TBinaryProtocol.TBinaryProtocol(transport)

	    client = ThriftHive.Client(protocol)
	    transport.open()

	    # reloadTables(client)

	    client.execute(query)
	    result = client.fetchAll()
	    
	    transport.close()

	    print result
	    return result

	except Thrift.TException, tx:
	    print '%s' % (tx.message)

def reloadTables(client):
	client.execute('DROP TABLE `barcelonamapprevious`')
	client.execute("CREATE EXTERNAL TABLE `barcelonamapprevious` (areaIniOccupiedTime int, iniWaitingTime int, runDistance int,id int,waitingtimef int,oldLong double,`long` double,statusf int,pickLong double,runlengthf int,dtime int,areanumPic int,status int,areaSearchingTime int,areaTime int,park int,areanum int,searchlengthf int,oldLat double,intervalIniWaitTime int,uniqueid string,lat double,pickLat double,areaPercentage double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/smartaxi/barcelona-map-previous'")
	client.execute('DROP TABLE `barcelonamoneyprevious`')
	client.execute("CREATE EXTERNAL TABLE `barcelonamoneyprevious` (areaIniOccupiedTime int, iniWaitingTime int, runDistance int,id int,waitingtimef int,oldLong double,`long` double,statusf int,pickLong double,runlengthf int,dtime int,areanumPic int,status int,areaSearchingTime int,areaTime int,park int,areanum int,searchlengthf int,oldLat double,intervalIniWaitTime int,uniqueid string,lat double,pickLat double,areaPercentage double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/smartaxi/barcelona-money-previous'")
	client.execute('DROP TABLE `barcelonakmeanprevious`')
	client.execute("CREATE EXTERNAL TABLE `barcelonakmeanprevious` (areaIniOccupiedTime int, iniWaitingTime int, runDistance int,id int,waitingtimef int,oldLong double,`long` double,statusf int,pickLong double,runlengthf int,dtime int,areanumPic int,status int,areaSearchingTime int,areaTime int,park int,areanum int,searchlengthf int,oldLat double,intervalIniWaitTime int,uniqueid string,lat double,pickLat double,areaPercentage double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/smartaxi/barcelona-kmean-previous'")