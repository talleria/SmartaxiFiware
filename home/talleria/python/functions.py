import csv
import sys
import math
import taxi



# """
# Lee los datos  del fichero que se le pasa a la funcion y lo guarda en una lista
# @parms filename: Nombre del archivo donde se leen los datos.
# @parms header: Valor booleano que indica si el fichero tiene o no cabecera.
# @return Devuelve una lista con los datos que contiene el fichero
# """
def readCsv(filename,header = True):
    rows = []
    try:
        f = open(filename, 'rb')
        reader = csv.reader(f)
        if header:
            reader.next()

        for row in reader:
            rows.append(row)
        f.close()
        
    except csv.Error, e:
        f.close()
        sys.exit('file %s, line %d: %s' % (filename, reader.line_num, e))            
    except IOError, e:
        sys.exit('file %s, %s' % (filename,e))
        return
    except StopIteration:       
        f.close()
        return rows
    return rows

# """
# Escribe los datos en un fichero. Los datos deben de estar en un diccionario
# @parms filename: Nombre del archivo donde se escribiran los datos.
# @parms lRows: Diccionario con todos los datos.
# @parms mode: modo en que quieres abrir el archivo.
# @parms header: Valor booleano que indica si quieres escribir o no la cabecera.
# """
def writeData2Csv(filename,lRows, mode,header):
    f = open(filename, mode)
    columns = lRows[0].keys()
    csvf = csv.DictWriter(f, columns, restval='', extrasaction='raise')
    
    if header:
        csvf.writeheader()
        
    csvf.writerows(lRows)
    
    f.close()

def writeFiles(fileName,text):
    f = None
    try:
    # This will create a new file or **overwrite an existing file**.
        f = open(fileName, "a")
        f.writelines(text) # Write a sequence of strings to a file

    except IOError, e:
        sys.exit('file %s, %s' % (fileName,e))
    
    finally:
        f.close()
        
        
def calcDistance(geoPoints):
    curLat = math.radians(float(geoPoints['lat']))
    prevLat = math.radians(float(geoPoints['oldLat']))
    
    curLon = math.radians(float(geoPoints['long']))
    prevLon = math.radians(float(geoPoints['oldLong']))
    
    x = (curLon - prevLon) * math.cos((prevLat+curLat)/2)
    y = curLat - prevLat
    
    move = (math.hypot(x,y) * 6371) #Devuelve en KM

    return move

# """
# Genera un diccionario donde la clave es el valor del elemnto groupBy y el valor un diccionario de objetos Car
# parms@  nameFile: ruta del fichero de donde se obtendran los valores insertados
# parms@  groupBy: nombre de la columna que se va a coger como clave
# return@ In diccionario que tiene como clave los diferentes valores de la columna groupBy y de valor tiene un diccionario de taxis
# """
def getTaxisGroupBy(nameFile,groupBy,statusF = -1):
    taxisByInterval = {}
    
    try:
        with open(nameFile) as f:
            taxis = taxi.CarPool()
            taxis.get_cars_from_csv(nameFile,True)
       
    except IOError as e:
        error = "No existe el fichero: {0} \n".format(nameFile)
        sys.stderr.writelines(error)
        sys.exit()
    
    for car in taxis.cars.values():
        if statusF == -1:
            if car.state[groupBy] not in taxisByInterval:
                taxisByInterval[car.state[groupBy]] = {}
            
            idTaxi = str(car.state[groupBy])+"_"+str(car.id)
            taxisByInterval[car.state[groupBy]][idTaxi] = car
      
        elif int(car.state['statusf']) == statusF:
            if car.state[groupBy] not in taxisByInterval:
                taxisByInterval[car.state[groupBy]] = {}
            
            idTaxi = str(car.state['dtime'])+"_"+str(car.id)
            taxisByInterval[car.state[groupBy]][idTaxi] = car
    return taxisByInterval

# """
# Inserta en todas las filas de los aggregados una key con el nameCol, y con el value valCol
# parms@  aggregates: lista de aggregates 
# parms@  nameCol: nombre de la columna 
# parms@  valCol: valor de la columna
# """
def insertColumAggregado(aggregates,nameCol,valCol):
    for aggregate in aggregates:
        aggregate[nameCol] = valCol