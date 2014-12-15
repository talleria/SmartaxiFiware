import time
from datetime import date
import csv, sys
import syslog
import numpy as np

import taxi
import ytaxapi
import datos as d
import mysql
import mysqlExt
import functions
from ContextBroker import ContextBroker


def taxisMoscu(city,layer):
    PREV_STATE_CSV = '{0}/{1}/{2}/previo.csv'.format(d.citiesDirectory,city,layer)   # CSV file with results of previous run
    HISTORY_CSV = '{0}/{1}/{2}/Previous/Previous-{3}.csv'.format(d.citiesDirectory,city,layer,date.today())  # CSV with all results from today
    JSON = '{0}/{1}/{2}/Previous/json-{3}.csv'
    

    SCRAP_LIM = 5000  # Max number of cars in Yandex response
        
    #Sacamos el perimetro
    row = mysql.getListData("select max(maxlat),min(minlat),max(maxlong),min(minlong) from `{0}-{1}-Areas`".format(city,layer))
    lat_max = row[0][0]
    long_max = row[0][2]
    lat_min = row[0][1]
    long_min = row[0][3]
    
    #Sacamos las areas
    areas = mysql.getListData("select * from `{0}-{1}-Areas`".format(city,layer))
    areas = np.asarray(areas)
    
    #Cerramos la conexion
    mysql.closeConnection()

    #we need to obtain all cars in given area from Yandex
    response = ytaxapi.get_y_area(lat_max, long_max, lat_min, long_min, SCRAP_LIM)
    
    
    new_set = taxi.CarPool()
    try:
        new_set.get_cars_from_json(response)
    except ValueError,e:
        #Escribe el json fallado 
        nameJson = JSON.format(d.citiesDirectory,city,layer,int(time.time())) 
        functions.writeFiles(nameJson,response)        
        mensaje = "Error lectura json %s, el json malo se guarda como  %s \n" % (time.strftime("%d %b %Y %H:%M:%S"),nameJson)
        functions.writeFiles(d.logDirectory+"/pythonData.log",mensaje)
        
        #Reintento
        time.sleep(2)
        response = ytaxapi.get_y_area(lat_max, long_max, lat_min, long_min, SCRAP_LIM)
        new_set.get_cars_from_json(response)
        nameJson = JSON.format(d.citiesDirectory,city,layer,int(time.time())) 
        functions.writeFiles(nameJson,response)
        
    
    #load previous results
    try:
        with open(PREV_STATE_CSV) as f:
            old_set = taxi.CarPool()
            old_set.get_cars_from_csv(PREV_STATE_CSV)

    except IOError as e:
        new_set.classify_by_area(areas)
        new_set.classify_cars()
        new_set.write_cars_to_csv(PREV_STATE_CSV,'wb',False)
        exit()

    new_set.classify_by_area(areas)
    new_set.load_prev_data(old_set.cars)
    new_set.classify_cars()

    # Clasifica los taxis por la area del pick
    if layer == "Money" or layer == "Kmean":
        new_set.classify_by_areaPick(areas)
    print "Calculos de python OK"

    #store results
    try:
        new_set.write_cars_to_csv(PREV_STATE_CSV,'wb',False)
        new_set.write_cars_to_csv(HISTORY_CSV,'ab',True)
        print "Calculos escritos en csvs OK"
    except IOError as e:
        raise e
    

def taxisBarcelona(city,layer):
    PREV_STATE_CSV = '{0}/{1}/{2}/previo.csv'.format(d.citiesDirectory,city,layer)   # CSV file with results of previous run
    HISTORY_CSV = '{0}/{1}/{2}/Previous/Previous-{3}.csv'.format(d.citiesDirectory,city,layer,date.today())  # CSV with all results from today
    
    #Creamos el objeto CarPool
    curTaxis = taxi.CarPool()
    
    #Sacamos las areas
    areas = mysql.getListData("select * from `{0}-{1}-Areas`".format(city,layer))
    mysql.closeConnection()
    areas = np.asarray(areas)

    # timeRef = int(sys.argv[3])
    #Calculamos el intervalo que consular
    timeRef = int(time.time())
    timeRef = timeRef - timeRef%300 -1
    
    lApiData = getDataContextBroker(timeRef-299, timeRef)
    
    if len(lApiData) > 0:
        curTaxis.getCarsFromExtSrc(lApiData,0);       #inserta los datos del servidor de api en el objeto
        print "En API hay: "+str(len(lApiData))+" filas de puntos crudos\n"

    # SI NO HAY DATOS
    if len(lApiData) == 0  and len(lSdbData) == 0:
        sys.stderr.writelines("No hay puntos ni en el servidor de API ni en SDB para el intervalo {0} --> {1} \n".format(timeRef-299,timeRef))
        sys.exit


    #Clasifica los taxis por areas
    curTaxis.classify_by_area(areas)
    
    #Trae los datos previos
    try:
        with open(PREV_STATE_CSV) as f:
            prevTaxis = taxi.CarPool()
            prevTaxis.get_cars_from_csv(PREV_STATE_CSV)
       
    except IOError as e:
        curTaxis.classify_cars()
        curTaxis.write_cars_to_csv(PREV_STATE_CSV,'wb',False)
        exit()    
    
    print "Taxis Previos: "+str(len(prevTaxis.cars.values()))
    print "Taxis Actuales: "+str(len(curTaxis.cars.values()))
    
    curTaxis.load_prev_data(prevTaxis.cars)
    curTaxis.classify_cars()

    # Clasifica los taxis por la area del pick
    # if layer == "Money" or layer == "Kmean":
    curTaxis.classify_by_areaPick(areas)    
    print "Calculos de python OK"

    #store results
    try:
        curTaxis.write_cars_to_csv(PREV_STATE_CSV,'wb',False)    
        curTaxis.write_cars_to_csv(HISTORY_CSV,'ab',True)
        print "Calculos escritos en csvs OK"
    except IOError as e:
        raise e

def getDataContextBroker(timeIni,timeFin):
    print "INTERVAL TIME: ",timeIni," , ",timeFin

    user = ''       #Insert User
    password = ''   #Insert Pass

    cb = ContextBroker(user, password)
    taxis = cb.query("Taxi")

    tupTaxis = ()

    # Procesamos todos los taxis que hay en el CB
    for taxi in taxis:
        aTaxi = [0,0,0,0,0]
        
        if(len(taxi) == 5):
            # Procesamos elemento a elemento de la tupla taxi
            for elementTaxi in taxi:

                if( str(elementTaxi[1]) == 'taxiId'):
                    aTaxi[0] = str(elementTaxi[2])

                # Miramos si el tiempo del taxi corresponde al intervalo que toca
                elif(str(elementTaxi[1]) == 'time'):
                    time = long(elementTaxi[2])
                    
                    if(time >= timeIni and time <= timeFin):
                        aTaxi[1] = str(time)
                    else:
                        break
                        
                elif(str(elementTaxi[1]) == 'lat'):
                    aTaxi[2] = str(elementTaxi[2])
                
                elif(str(elementTaxi[1]) == 'lon'):
                    aTaxi[3] = str(elementTaxi[2])
                
                elif(str(elementTaxi[1]) == 'status'):
                    aTaxi[4] = str(elementTaxi[2])
            
            # Anaditmos a la lista final solo si el idTaxi y el tiempo son mayor que 0
            if(aTaxi[0] != 0 and aTaxi[1] != 0):
                tupTaxis = tupTaxis + (tuple(aTaxi),)

    print "TUPLAS: ",tupTaxis
    print "len tup: ",len(tupTaxis)," len source: ",len(taxis)

    return tupTaxis;

def prev2Csv(city,layer):
    if(city == "Moscu"):
        taxisMoscu(city,layer)
    elif(city == "Barcelona"):    
        taxisBarcelona(city,layer)     

prev2Csv(sys.argv[1],sys.argv[2])



