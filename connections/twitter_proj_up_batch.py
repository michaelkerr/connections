#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# Twitter project specific connections update, batch #
#################
# Created Date: 2014/01/28
# Last Updated: 2014/01/31

### Resources ###
import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

mongoclient = MongoClient('192.168.1.152', 27017)
mongo_db = mongoclient['connections']
author_collection = mongo_db['authorcons']
entity_collection = mongo_db['entitycons']


### Classes ###


### Functions ###
def recent_object_id():
	start_date = list(author_collection.find().sort('_id', 1).limit(1))[0]['_id'].generation_time
	## >Get just the YYYY-MM-DD
	start_date = (start_date - datetime.timedelta(days=1)).strftime("%Y, %m, %d")
	## >Convert to a list of ints
	start_date = list(int(s) for s in start_date.split(','))
	## >Create the new time for the mongo ObjectId
	id_time = int((datetime.datetime(start_date[0], start_date[1], start_date[2]) - datetime.datetime(1970, 1, 1)).total_seconds())
	## >Set the other identifiers in the ObjectId to 0
	machine, pid, inc = 0, 0, 0

	## >Recompose an ObjectId string
	new_objectid = hex(id_time).replace('0x', '')
	new_objectid += hex(machine).replace('0x', '').zfill(6)
	new_objectid += hex(pid).replace('0x', '').zfill(4)
	new_objectid += hex(inc).replace('0x', '').zfill(6)
	return new_objectid


def daily_object_id(start_date):
	## >Convert to a list of ints
	start_date = list(int(s) for s in str(start_date).split('-'))
	## >Create the new time for the mongo ObjectId
	id_time = int((datetime.datetime(start_date[0], start_date[1], start_date[2]) - datetime.datetime(1970, 1, 1)).total_seconds())
	## >Set the other identifiers in the ObjectId to 0
	machine, pid, inc = 0, 0, 0

	## >Recompose an ObjectId string
	new_objectid = hex(id_time).replace('0x', '')
	new_objectid += hex(machine).replace('0x', '').zfill(6)
	new_objectid += hex(pid).replace('0x', '').zfill(4)
	new_objectid += hex(inc).replace('0x', '').zfill(6)
	return new_objectid


### Main ###
start_time = datetime.datetime.now()
total_processed = 0

## >Get date range
start_date = datetime.date(2014, 1, 1)
end_date = datetime.date(2014, 1, 31)
day = datetime.timedelta(days=1)
daterange = lambda d1, d2: (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))

## >For each day in the time period
for doc_date in daterange(start_date, end_date):
	print 'Processing ' + str(doc_date) + ' for project matching/scoring updates'
	## >Create an objectid for the start date
	start_objectid = daily_object_id(doc_date)
	## >Create an objectid for the end date
	end_objectid = daily_object_id(doc_date + datetime.timedelta(days=1))

	## >Get the documents that are in between those date by objectid
	con_docs = list(author_collection.find({
					'_id': {'$gte': ObjectId(start_objectid), '$lt': ObjectId(end_objectid)},
					'Network': 'twitter'}
					))

	## >If there are any new tweets
	if len(con_docs) > 0:
		total_processed += len(con_docs)
		for entry in con_docs:
			## >Get the project information
			projects = []
			for item in entry['Meta']['projects']:
				projects.append(item)
			if len(projects) > 0:
				entry['Matching'] = []
				for project in projects:
					project_dict = {}
					project_dict[u'ProjectName'] = project
					project_dict[u'Topics'] = []
					entry[u'Matching'].append(project_dict)

			## >Get the topic information
			#TODO - no topic information, need project-->topic match
			topics = []
			for item in entry['Meta']['topics']:
				topics.append(item)
			if len(topics) > 0:
				for topic in topics:
					pass

			author_collection.update({'_id': entry['_id']}, {'$set': {u'Matching': entry['Matching']}}, upsert=False)

print 'Updated :' + str(total_processed) + ' connections'
