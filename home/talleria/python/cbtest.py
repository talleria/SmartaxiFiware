from ContextBroker import ContextBroker

user = ''		# Insert User
password = '' 	# Insert Password

cb = ContextBroker(user, password)

taxis = cb.query("Taxi")
tupTaxis = ()

timeFin = int(time.time())
timeFin = timeFin - timeFin%300 -1
timeIni = timeFin - 299;
print "INTERVAL TIME: ",timeIni," , ",timeFin

# Procesamos todos los taxis que hay en el CB
for taxi in taxis:
	aTaxi = [0,0,0,0,0]
	
	if(len(taxi) == 5):

		# Procesamos elemento a elemento de la tupla taxi
		for elementTaxi	in taxi:

			if(	str(elementTaxi[1]) == 'taxiId'):
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