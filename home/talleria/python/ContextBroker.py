# -*- coding: utf-8 -*-

import urllib2
import json

class ContextBroker(object):
	"""Easy interface from Orion Context Broker to Python"""

	# TODO Save token and check if expired

	AUTH_URL = 'http://cloud.lab.fi-ware.org:4730/v2.0/tokens'
	REQUEST_URL = 'http://orion.lab.fi-ware.org:1026/'

	def __init__(self, user, password):
		self.user = user
		self.password = password

	def getToken(self):
		authPost = json.dumps( {'auth':{'passwordCredentials':{'username':self.user,'password':self.password}}} )
		authReq = urllib2.Request(ContextBroker.AUTH_URL, authPost)
		authResp = urllib2.urlopen(authReq)
		authJson = json.loads(authResp.read())
		return authJson["access"]["token"]["id"]

	def query(self, type, isPattern = True, id = '.*'):
		queryPost = json.dumps( {"entities":[{"type":type,"isPattern":isPattern,"id":id}]} )
		headers = {
			"x-auth-token" : self.getToken(),
			"Content-Type" : "application/json",
			"Accept" : "application/json"
		}
		queryReq = urllib2.Request(ContextBroker.REQUEST_URL + 'NGSI10/queryContext', queryPost, headers)
		queryResp = urllib2.urlopen(queryReq)
		queryJson = json.loads(queryResp.read())

		elements = []

		for elementJson in queryJson["contextResponses"]:
			
			element = []

			for attributeJson in elementJson["contextElement"]["attributes"]:
				attribute = ( attributeJson["type"], attributeJson["name"], attributeJson["value"] )
				element.append(attribute)

			elements.append(element)

		return elements