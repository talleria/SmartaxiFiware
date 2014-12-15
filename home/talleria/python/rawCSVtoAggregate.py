import csv
import syslog
import sys
import taxi
import datos as d
import mysql
from datetime import datetime

def insertDataInBDExtends(nameTable,data,time):
    numTaxis = 0
    query = 'INSERT INTO `{0}`' \
            '(dtime, areanum, statusf, aggregate, queuelength, minwait, meanwait, ' \
            'maxwait, meanrun, maxrun, meansearch, maxsearch, minareasearchingtime,meanareasearchingtime,maxareasearchingtime, ' \
            'minareatime,meanareatime,maxareatime, minareainioccupiedtime, meanareainioccupiedtime,maxareainioccupiedtime, ' \
            'minareapercentage,meanareapercentage,maxareapercentage) VALUES '.format(nameTable)
    for s in data:
        if int(s['areanum'])==9999:
            continue
         
        # time = datetime.utcfromtimestamp(float(s['dtime']))
        sTime = datetime.utcfromtimestamp(float(time))
        query += '("{0:%Y-%m-%d %H:%M:%S}",{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}), '.format(
            sTime,            
            # s['dtime'],
            s['areanum'],
            s['statusf'],
            s['aggregate'],
            s['queuelength'],
            s['minwait'],
            s['meanwait'],
            s['maxwait'],
            s['meanrun'],
            s['maxrun'],
            s['meansearch'],
            s['maxsearch'],        
            s['minareaSearchingTime'],
            s['meanareaSearchingTime'],
            s['maxareaSearchingTime'],
            s['minareaTime'],
            s['meanareaTime'],
            s['maxareaTime'],       
            s['minareaIniOccupiedTime'],
            s['meanareaIniOccupiedTime'],
            s['maxareaIniOccupiedTime'],
            s['minareaPercentage'],
            s['meanareaPercentage'],
            s['maxareaPercentage'],
        )
        numTaxis+=1 

    if numTaxis > 0:
        query = query.rstrip(' ,')
        mysql.execute(query)
    print "Se han insertado "+str(numTaxis)+" filas en la tabla "+nameTable

   
            
def insertDataInBD(nameTable,data,time):
    numTaxis = 0
    query = 'INSERT INTO `{0}`' \
        '(dtime, areanum, statusf, aggregate, queuelength, minwait, meanwait, ' \
        'maxwait, meanrun, maxrun, meansearch, maxsearch) VALUES '.format(nameTable)
    
    for s in data:
        if int(s['areanum'])==9999:
            continue
        
        # convierte el timestamp en un datetime para insertar en formato estring en la tabla
        # time = datetime.utcfromtimestamp(float(s['dtime']))
        sTime = datetime.utcfromtimestamp(float(time))
        query += '("{0:%Y-%m-%d %H:%M:%S}",{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}), '.format(
            sTime,            
            # s['dtime'],
            s['areanum'],
            s['statusf'],
            s['aggregate'],
            s['queuelength'],
            s['minwait'],
            s['meanwait'],
            s['maxwait'],
            s['meanrun'],
            s['maxrun'],
            s['meansearch'],
            s['maxsearch']
        )

        numTaxis+=1 
    if numTaxis > 0:
        query = query.rstrip(' ,')
        mysql.execute(query)
    print "Se han insertado "+str(numTaxis)+" filas en la tabla "+nameTable

def insertDataKmeanInBD(nameTable,data,time):
    numTaxis = 0
    query = 'INSERT INTO `{0}`' \
        '(dtime, areanum, statusf, aggregate, queuelength, minwait, meanwait, ' \
        'maxwait, meanrun, maxrun, meansearch, maxsearch) VALUES '.format(nameTable)
    for s in data:
        if int(s['areanumPick'])==9999:
            continue
        
        # convierte el timestamp en un datetime para insertar en formato estring en la tabla
        # time = datetime.utcfromtimestamp(float(s['dtime']))
        sTime = datetime.utcfromtimestamp(float(time))
        query += '("{0:%Y-%m-%d %H:%M:%S}",{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}), '.format(
            sTime,            
            # s['dtime'],
            s['areanumPick'],
            s['statusf'],
            s['aggregate'],
            s['queuelength'],
            s['minwait'],
            s['meanwait'],
            s['maxwait'],
            s['meanrun'],
            s['maxrun'],
            s['meansearch'],
            s['maxsearch']
        )
        numTaxis+=1 
 
    if numTaxis > 0:
        query = query.rstrip(' ,')
        mysql.execute(query)
    print "Se han insertado "+str(numTaxis)+" filas en la tabla "+nameTable


def insertDataMoneyInBD(nameTable,data,time):
    numTaxis = 0
    query = 'INSERT INTO `{0}`' \
        '(dtime, areanum, statusf, aggregate, meanRunDistance) VALUES '.format(nameTable)
    
    for s in data:
        if int(s['areanumPick'])==9999:
            continue
        
        # convierte el timestamp en un datetime para insertar en formato estring en la tabla
        # sTime = datetime.utcfromtimestamp(float(s['dtime']))
        sTime = datetime.utcfromtimestamp(float(time))
        query += '("{0:%Y-%m-%d %H:%M:%S}",{1},{2},{3},{4}), '.format(
            sTime,            
            # s['dtime'],
            s['areanumPick'],
            s['statusf'],
            s['aggregate'],
            s['meanRunDistance']
        )
        numTaxis+=1 
    if numTaxis > 0:
        query = query.rstrip(' ,')
        mysql.execute(query)
    print "Se han insertado "+str(numTaxis)+" filas en la tabla "+nameTable



def csv2MapAggregates(city,layer):
    # CSV file with results of previous run
    PREV_STATE_CSV = '{0}/{1}/{2}/previo.csv'.format(d.citiesDirectory,city,layer) 
    SDB_CSV = '{0}/{1}/sdbData.csv'.format(d.citiesDirectory,city) 

    # MYSQL_TABLE ="{0}-{1}-AggregatesExtends".format(city,layer)
    MYSQL_TABLE ="{0}-{1}-Aggregates".format(city,layer)
    MYSQL_TABLE_EXTENDS ="{0}-{1}-AggregatesExtendsTotal".format(city,layer)

    grouping = ('areanum', 'statusf')

    # crea el objeto y lee el csv previo
    taxis = taxi.CarPool()
    taxis.get_cars_from_csv(PREV_STATE_CSV,False)

    if not taxis.cars:
        print "El archivo de datos pretratados para "+city+" , "+layer+" estan vacios"
        return

    
    #calculate aggregation
    metrics = taxis.make_aggregation(taxis.cars,grouping)
    
    #Inserta los datos normales en la tabla Aggregates
    insertDataInBD(MYSQL_TABLE,metrics,taxis.curtime)
    
    #Inserta los datos normales en la tabla AggregatesExtendsTotal
    insertDataInBDExtends(MYSQL_TABLE_EXTENDS,metrics,taxis.curtime)    

    #if there was no errors - send message to syslog
    syslog.syslog('Successful execution')
    

def csv2KmeansAggregates(city,layer):
    
    MYSQL_TABLE ="{0}-{1}-Aggregates".format(city,layer)
    PREV_STATE_CSV = '{0}/{1}/{2}/previo.csv'.format(d.citiesDirectory,city,layer)

    grouping = ('areanumPick', 'statusf')
    
    taxis = taxi.CarPool()
    taxis.get_cars_from_csv(PREV_STATE_CSV,False)

    aggregatesData = taxis.make_aggregation(taxis.cars,grouping)
    
    insertDataKmeanInBD(MYSQL_TABLE,aggregatesData,taxis.curtime)    

def csv2MoneyAggregates(city,layer):
    
    MYSQL_TABLE ="{0}-{1}-Aggregates".format(city,layer)
    PREV_STATE_CSV = '{0}/{1}/{2}/previo.csv'.format(d.citiesDirectory,city,layer)

    grouping = ('areanumPick', 'statusf')
    
    taxis = taxi.CarPool()
    taxis.get_cars_from_csv(PREV_STATE_CSV,False)

    aggregatesData = taxis.make_aggregation(taxis.cars,grouping)

    insertDataMoneyInBD(MYSQL_TABLE,aggregatesData,taxis.curtime)    
    
def csv2Aggregates(city,layer):
    if layer=="Map":
        csv2MapAggregates(city,layer)
    elif layer == "Kmean":
        csv2KmeansAggregates(city,layer) 
    elif layer == "Money":
        csv2MoneyAggregates(city,layer)



csv2Aggregates(sys.argv[1],sys.argv[2])
