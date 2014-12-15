import os
import sys
import taxi

from datetime import date
from datetime import timedelta

import functions as f
import datos as d


def makeHistAggByIniWaitingTime(city,layer,srcDir):
	dirs = os.listdir(srcDir+"/Previous")
	dirs.sort()

	for srcFile in dirs:
		baseName = srcFile.split('_',1)
		if len(baseName) != 2:
			continue

		namePrevious = srcDir+"/Previous/"+srcFile
		nameAggregates = srcDir+"/Corrected_IniWaitingTime/Corrected_IniWaitingTime_"+baseName[1]

		print namePrevious
		print nameAggregates
		print "\n"

		#Cargo el fichero en un diccionario como clave intervalIniWaitTime
		taxisByInterval = f.getTaxisGroupBy(namePrevious,"intervalIniWaitTime",taxi.PICK)

		makeAggByIniWaitingTime(taxisByInterval,nameAggregates,layer)


def aggregatesByIniWaitingTime(city,layer):
	# Nombre de los archivos
	time = date.today() - timedelta(days=1)
	namePrevious = '{0}/{1}/{2}/Previous/Previous-{3:%Y-%m-%d}.csv'.format(d.citiesDirectory,city,layer,time)
	nameAggregates = '{0}/{1}/{2}/Corrected_IniWaitingTime/Corrected_IniWaitingTime_{3:%Y-%m-%d}.csv'.format(d.citiesDirectory,city,layer,time)

	#Cargo el fichero en un diccionario como clave intervalIniWaitTime
	taxisByInterval = f.getTaxisGroupBy(namePrevious,"intervalIniWaitTime",taxi.PICK)

	makeAggByIniWaitingTime(taxisByInterval,nameAggregates,layer)



def makeAggByIniWaitingTime(taxisByInterval,nameAggregates,layer):
	header = True;
	grouping = ('areanumPick','statusf')

	#Ordeno los elementos del array para que sea descendente
	keys = taxisByInterval.keys()
	keys.sort()

	total = 0
	for key in keys:
		if key == '0': 
			continue

		# Saco los datos del intervalo
		intervalCars = taxisByInterval[key]
		print key+" "+str(len(intervalCars))
		total += len(intervalCars)

		# Genero los aggregados para ese intervalo
		taxis = taxi.CarPool()
		aggregates = taxis.make_aggregation(intervalCars,grouping)
		f.insertColumAggregado(aggregates,"intervalIniWaitTime",key)
		
		#Escribo los aggregados en el fichero
		f.writeData2Csv(nameAggregates,aggregates,'ab',header)
		
		header = False

	print "Total: "+str(total)



# srcDir = "/var/www/server/cities/Barcelona/Kmean"
# makeHistAggByIniWaitingTime(city,layer,srcDir)



aggregatesByIniWaitingTime(sys.argv[1],sys.argv[2])