#!/usr/bin/python
# coding: utf-8
#
# This file contains main logics for dealing with cars obtained from the Api:
# parsing of cars, calculation of the metrics and preparations to store them in db.

import csv
import sys
import time
import math
import json
import os.path
import numpy as np
from matplotlib import mlab
import datos as d
import functions as f

header = True

OCUPIED = 0
PICK    = 1
DROP    = 2
FREE    = 3

class CarPool(object):
    """
    class for the set of cars. It provides functions to treat all cars at once.
    """

    def __init__(self):
        self.cars = {}
        # self.stops = {}
        # self.areas = {}
        self.picks = {}
        self.curtime = int(time.time())

    # """
    # parse json response form Yandex to a flat list of all cars
    # """
    def get_cars_from_json(self, json_resp):
        lParks = json.loads(json_resp)
        
        for park in lParks:           
            for car_src in park['cars']:
                car = Car(self.curtime)
                car.get_from_yresp(car_src, park['parkid'])
                self.cars[car.id] = car
                
        return self.cars
    
    # """
    # parse Tuplas of Cars to a dict with all cars
    # """
    def getCarsFromExtSrc(self, lCars,park, append=False):
        # TODO AÇO PERQUE???
        if not append:
            self.cars = {}
            

        for taxi in lCars:            
            car = Car(self.curtime)
            car.getCarFromExtSrc(taxi,park)
            self.cars[car.id] = car
        return self.cars

    def write_cars_to_csv(self, filename, mode,historyFile):
        self.writeCarsData2Csv(filename,self.cars,mode,historyFile)
        
    # """
    # writes all cars (current states of cars) to specified file
    # """
    def writeCarsData2Csv(self, filename,data, mode,historyFile):
        global header
        #Es para que solo escriba la cabecera la primera vez
        if historyFile and os.path.isfile(filename):            
            header = False
            
        f = open(filename, mode)
        csvf = csv.DictWriter(f, Car.PARS.keys(), restval='', extrasaction='raise')
        
        if header:
            csvf.writeheader()

        for car in data.values():
            csvf.writerow(car.get_state_dict())
        f.close()
        
        
        header = True
        

    # """
    # Lee las filas del archivo introducido y lo carga en el diccionario cars
    # parms@  filename el nombre del archivo que queremos abrir
    # parms@  repeated si quieres cargar cars con el mismo id, recrea un id a partir del id+"_"+tiempo
    # return@ devuel ve la lista de taxis. cars
    # """
    def get_cars_from_csv(self,filename,repeated = False):
        try:
            f = open(filename, 'rb')
            reader = csv.reader(f)
            if header:
                reader.next()

            for row in reader:
                car = Car()
                car.get_from_csv(row)

                if repeated:
                    id = car.id+"_"+str(car.state['dtime'])
                    self.cars[id] = car                
                else:
                    self.cars[car.id] = car                
           
        except csv.Error, e:
            f.close()
            sys.exit('file %s, line %d: %s' % (filename, reader.line_num, e))
            
        except IOError, e:
            return
        except StopIteration:            
            f.close()
            return
        return self.cars
    
    # """
    # Carga la lista de taxis en los diccionarios
    # """        
    def getCarsFromPreviousHist(self,rows):
        for row in rows:
            car = Car()
            car.get_from_csv(row)
            self.cars[car.id] = car
           
        return self.cars
    

    def getSdbCsv(self,fileName):
        try:
            f = open(fileName, 'rb')
            reader = csv.reader(f)
            header = reader.next()

            for row in reader:
                car = Car()
                car.get_sdb_csv(row,header)
                self.cars[car.id] = car

        except csv.Error, e:
            f.close()
            sys.exit('file %s, line %d: %s' % (fileName, reader.line_num, e))

        except IOError:
            return
        except StopIteration:            
            f.close()
            return

        # print self.cars
        return self.cars


    # """
    # distributes cars to specific areas
    # """
    def classify_by_area(self, areas):
        for car in self.cars.values():
            car.get_area(areas)
    
    # """
    # distributes cars to specific areas of pickLat , pickLong
    # """
    def classify_by_areaPick(self, areas):
        for car in self.cars.values():
            car.get_areaPick(areas)


    # """
    # on the base of dict with previous state data
    # assign prev_state to cars (if it is for particular car)
    # """
    def load_prev_data(self, prev_cars):
        for car in self.cars.values():
            if car.id in prev_cars:
                car.prevstate = prev_cars[car.id].state

    # """
    # actually make calculations of the car's parameters
    # """
    def classify_cars(self):
        for car in self.cars.values():
            car.classify()

    # """
    # calculate some statistics metrics on current state
    # ! should be called only after classify_cars()
    # """
    def make_aggregation(self,lista,grouping):
        #Saco el timestamp del primer taxi 
        time =  lista.values()[0].state['dtime']
        #time = calendar.timegm(time.timetuple())
        
        #great deal to obtain proper data structure for numpy operations
        #can be much more efficient
        pars_list = []
        for item in Car.PARS:
            pars_list.append(item)

        dtype = []
        for par in pars_list:
            dtype.append((par, Car.PARS[par]))
        
        # print pars_list
        # print dtype

        all_states = []
        for car in lista.values():
             all_states.append(tuple([car.state[par] for par in pars_list]))        
        # print all_states

        arr = np.rec.array(all_states, dtype)
        # print arr,"\n"

        def queue(x):
            return np.sum((x > 1))

        # Agrega por area o areaPic dependiendo de la capa que sea    
        # if layer == "Money":
        #     grouping = ('areanumPick', 'statusf')
        # else:
        #     grouping = ('areanum', 'statusf')

        #define which metrics to calculate with which function
        metrics = (('areanum', np.count_nonzero, 'aggregate'),
                   ('waitingtimef', np.min, 'minwait'),
                   ('waitingtimef', np.mean, 'meanwait'),
                   ('waitingtimef', np.max, 'maxwait'),

                   ('runlengthf', np.mean, 'meanrun'),
                   ('runlengthf', np.max, 'maxrun'),

                   ('runDistance', np.mean, 'meanRunDistance'),

                   ('searchlengthf', np.mean, 'meansearch'),
                   ('searchlengthf', np.max, 'maxsearch'),


                   ('waitingtimef', queue, 'queuelength'),                   
                   
                   ('areaSearchingTime',np.min,'minareaSearchingTime'),
                   ('areaSearchingTime',np.mean,'meanareaSearchingTime'),
                   ('areaSearchingTime',np.max,'maxareaSearchingTime'),
                   
                   ('areaTime',np.min,'minareaTime'),
                   ('areaTime',np.mean,'meanareaTime'),
                   ('areaTime',np.max,'maxareaTime'),
                   
                   ('areaIniOccupiedTime',np.min,'minareaIniOccupiedTime'),
                   ('areaIniOccupiedTime',np.mean,'meanareaIniOccupiedTime'),
                   ('areaIniOccupiedTime',np.max,'maxareaIniOccupiedTime'),
                   
                   ('areaPercentage',np.min,'minareaPercentage'),
                   ('areaPercentage',np.mean,'meanareaPercentage'),
                   ('areaPercentage',np.max,'maxareaPercentage'),
                   
            )
        #and actually make the calculations
        result=mlab.rec_groupby(arr, grouping, metrics)
        #print result
        
        #and convert it back to dictionary
        keys = result.dtype.names
        #print keys
        
        #return [dict(zip(keys, record)) for record in result]
        
        aggregates = [dict(zip(keys, record)) for record in result]
        # for aggregate in aggregates:
        #     aggregate['dtime'] = time
        f.insertColumAggregado(aggregates,'dtime',time)
        return aggregates

    
class Car (object):
    """
    class for car (taxi) instance. provides all functions to work with one
     taxi instance
    """
    DTIME_FMT = '%Y-%m-%d %H:%M:%S.%f'
    PARS = {
        'dtime': 'uint',
        'iniWaitingTime' : 'uint',
        'intervalIniWaitTime': 'uint',
        
        'park': 'uint',
        'id': 'S30',
        'uniqueid': 'S30',
        
        'long': 'float',
        'lat': 'float',
        'oldLong': 'float',
        'oldLat': 'float',
        'pickLong': 'float',
        'pickLat': 'float',
        
        'areanum': 'uint',
        'areanumPick': 'uint',
        'status': 'uint',
        'statusf': 'uint',
        
        'waitingtimef': 'uint',
        'searchlengthf': 'uint',
        
        'runlengthf': 'uint',
        'runDistance': 'float',
        
        'areaSearchingTime' : 'uint',
        'areaTime' : 'uint',
        'areaIniOccupiedTime' : 'uint',
        'areaPercentage' : 'float'
        
        #'speedf': 'float',
        #'nsf': 'float',
        #'ewf': 'float',
        #'ansf': 'float',
        #'aewf': 'float',
    }   

    def __init__(self, curtime=None):
        self.curtime = curtime
        self.prevstate = None

    # """
    # makes unique id for the car in our db (to make possible search for the previous states)
    # """
    def make_uniqueid(self):
        if hasattr(self, 'id'):
            return
        if 'park' and 'id' in self.state:
            self.id = '{0}salt{1}'.format(self.state['park'], self.state['id'])
            self.state['uniqueid'] = self.id

    # """
    # parses part of Yandex responce and fill self attributes
    # """
    def get_from_yresp(self, yresp, park):
        self.state = {
                    'park': park,
                    'id': yresp['uuid'],
                    'status': int(not int(yresp['free'])),  # status = 1 = occupied
                    'long': float(yresp['geopoint'][0]),
                    'lat': float(yresp['geopoint'][1]),
        }
        self.state['dtime'] = self.curtime
        self.make_uniqueid()
        return self
    
    def getCarFromExtSrc(self, row,park):
        self.state = {
            'park': park,
            'id': row[0],
            'status': int(row[4]), 
            'long': float(row[3]),
            'lat': float(row[2]),            
        }
        
        #Cuandrando los tiempos
        # self.state['dtime'] = (int(row[1]))

        self.state['dtime'] = self.curtime
        self.make_uniqueid()
        return self

    def get_from_csv(self, row):
        self.state = dict(zip(self.PARS.keys(), row))
        self.make_uniqueid()

        return self
    

    def get_sdb_csv(self, row,header):
        self.state = dict(zip(header, row))
        self.id = self.state['id']
        self.state['park'] = 0
        self.state['uniqueid'] = self.state['id']
        self.state['dtime'] = self.state['dtime']/1000.0
        
        #self.state['dtime'] = datetime.utcfromtimestamp(float(self.state['dtime'])/1000.0)
        return self

    # """
    # returns dictionary with current car's parameters
    # """
    def get_state_dict(self):
        self.make_uniqueid()
        d = self.state
        #d['dtime'] = self.timeStamp
        #d['dtime'] = time.mktime(d['dtime'].timetuple())
        #"""
        #Cuando escribe el previo ya transforma el datetime a timestamp, cuando escribe el historico ya esta en timestamp y peta. Por eso el try catch
        #"""
        #try:
        #    d['dtime'] = calendar.timegm(d['dtime'].timetuple())
        #except AttributeError:
        #    return d
       
        return d

    def get_area(self, areas):
        self.state['areanum'] = 9999
        for area in areas:            
            if area[1] > float(self.state['lat']) > area[2] and area[3] < float(self.state['long']) < area[4]:                
                self.state['areanum'] = int(area[0])

    def get_areaPick(self, areas):
        self.state['areanumPick'] = 9999
        for area in areas:            
            if area[1] > float(self.state['pickLat']) > area[2] and area[3] < float(self.state['pickLong']) < area[4]:                
                self.state['areanumPick'] = int(area[0])
             

    def classify(self):
        """
        Main function for calculation of the car's state parameters
        on the base of the current state and previous one.
        """
        #0-ocupied; 1- pick up; 2- drop; 3- free
        
        cur = self.state
        prev = self.prevstate
        #no previous state
        if prev is None:
            if int(cur['status']) == 1:
                cur['statusf'] = PICK
            else:
                cur['statusf'] = FREE

            cur['waitingtimef'] = 0
            cur['searchlengthf'] = 0
            cur['runlengthf'] = 0
            cur['areaSearchingTime'] = 0
            cur['areaTime'] = 0
            cur['areaIniOccupiedTime'] = 0
            cur['areaPercentage'] = 0
            cur['iniWaitingTime'] = 0
            cur['intervalIniWaitTime'] = 0
            cur['oldLat'] = 0
            cur['oldLong'] = 0
            cur['pickLat'] = 0
            cur['pickLong'] = 0
            cur['areanumPick'] = 9999
            cur['runDistance'] = 0
            #cur['speedf'] = 0.000000001
            #cur['nsf'] = 0.0
            #cur['ewf'] = 0.0
            #cur['ansf'] = 0.0
            #cur['aewf'] = 0.0                

        else:
            # We have the previous state for the car, so make the calculations with it
            difTimes = cur['dtime'] - int(prev['dtime'])
  
            cur['oldLat'] = float(prev['lat'])
            cur['oldLong'] = float(prev['long'])
           
            cur['waitingtimef'] = 0
           
            cur['iniWaitingTime'] = 0
            cur['intervalIniWaitTime'] = 0

            cur['searchlengthf'] = 0
            
            cur['runlengthf'] = 0
            cur['runDistance'] = 0
            cur['areanumPick'] = 9999

            cf = int(cur['status'])
            pf = int(prev['status'])
            prevArea = int(prev['areanum'])
            curArea = int(cur['areanum'])
            
      
            if cf == 1 and pf == 1:
                cur['statusf'] = OCUPIED
            elif cf == 1 and pf == 0:
                cur['statusf'] = PICK
            elif cf == 0 and pf == 1:
                cur['statusf'] = DROP
            elif cf == 0 and pf == 0:
                cur['statusf'] = FREE

            #Calculos PickLat,PickLong
            if int(cur['statusf']) == PICK:
                cur['pickLat'] = cur['oldLat']
                cur['pickLong'] = cur['oldLong']
                
            elif int(cur['statusf']) in [OCUPIED,DROP]:
                cur['pickLat'] = prev['pickLat']
                cur['pickLong'] = prev['pickLong']
                
            elif int(cur['statusf']) == FREE:                
                cur['pickLat'] = 0
                cur['pickLong'] = 0
             
            
            #Calculos de runDistance
            if int(cur['statusf']) == DROP and (float(cur['pickLat']) != 0 and float(cur['pickLong']) != 0):
                geoPoints = {'lat':cur['lat'],'long':cur['long'],'oldLat':cur['pickLat'],'oldLong':cur['pickLong']}
                cur['runDistance'] = f.calcDistance(geoPoints)  * 1.3          
                
                
            #Calculos de waitingTime
            if cur['statusf'] in [PICK, FREE]:
                geoPoints = {'lat':cur['lat'],'long':cur['long'],'oldLat':cur['oldLat'],'oldLong':cur['oldLong']}
                move = f.calcDistance(geoPoints)
                
                if move < 0.2:
                    cur['waitingtimef'] = int(prev['waitingtimef']) + difTimes
                
            #Calculos de iniWaitingTime & intervalIniWaitTime
            if int(cur['waitingtimef']) != 0:                
                cur['iniWaitingTime'] = cur['dtime'] - int(cur['waitingtimef'])
                cur['intervalIniWaitTime'] = int(cur['iniWaitingTime']) - (int(cur['iniWaitingTime']) % 300)
            
           #Calculos de SearchLength
            if cur['statusf'] in [PICK,FREE]:
                cur['searchlengthf'] = int(prev['searchlengthf']) + difTimes
         
           #Calculos de runLength
            if cur['statusf'] in [OCUPIED, DROP]:
               cur['runlengthf'] = int(prev['runlengthf']) + difTimes

            #cur['nsf'] = float(cur['lat']) - float(prev['lat'])
            #cur['ewf'] = float(cur['long']) - float(prev['long'])
            #cur['speedf'] = ((cur['nsf'] ** 2 + cur['ewf'] ** 2) ** 0.5 + 0.000000001) / difTimes
            #cur['ansf'] = cur['nsf'] / cur['speedf'] - float(prev['nsf']) / float(prev['speedf'])
            #cur['aewf'] = cur['ewf'] / cur['speedf'] - float(prev['ewf']) / float(prev['speedf'])          
                        
            if prevArea == curArea:
                
                if int(cur['statusf']) == FREE:
                    #Calculo tiempo busqueda por area
                    cur['areaSearchingTime'] =  int(prev['areaSearchingTime']) + difTimes
                    #Calculo tiempo de inicio ocupado
                    cur['areaIniOccupiedTime'] = 0
                    
                if int(cur['statusf']) == PICK:
                    cur['areaSearchingTime'] =  int(prev['areaSearchingTime']) + math.trunc(0.5 * difTimes)
                    cur['areaIniOccupiedTime'] = 0
                
                if int(cur['statusf']) == DROP:
                    cur['areaSearchingTime'] =  math.trunc(0.5 * difTimes)
                    cur['areaIniOccupiedTime'] = int(prev['areaIniOccupiedTime']) + math.trunc(0.5 * difTimes)
                
                if int(cur['statusf']) == OCUPIED:
                    cur['areaSearchingTime'] = 0
                    cur['areaIniOccupiedTime'] = int(prev['areaIniOccupiedTime']) + difTimes
                
                #Calculo tiempo total por area
                cur['areaTime'] = int(prev['areaTime']) + difTimes
                
            else:                
                if cur['statusf'] in [DROP, FREE]:
                    cur['areaSearchingTime'] = math.trunc(difTimes * 0.5)
                    cur['areaIniOccupiedTime'] = 0
                else:
                    cur['areaSearchingTime'] = 0
                    cur['areaIniOccupiedTime'] = math.trunc(difTimes * 0.5)
                    
                cur['areaTime'] = math.trunc(0.5 * difTimes)
                
            cur['areaPercentage'] = max(float(cur['areaSearchingTime']),0.00001)/max((float(cur['areaTime']) - float(cur['areaIniOccupiedTime'])),0.0001)
        self.state = cur

#function for the testing purposes
if __name__ == "__main__":
    import doctest
    doctest.testmod()