#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# JSON to graph, daily #
#################
# Created Date: 2014/01/28
# Last Updated: 2014/02/05

### Resources ###
#from bson.objectid import ObjectId
import datetime
#from py2neo import neo4j
from pymongo import MongoClient

## >MongoDB related
mongoclient = MongoClient('XXX.XXX.XXX.XXX', 27017)
mongo_db = mongoclient['connections']
author_collection = mongo_db['authorcons']
entity_collection = mongo_db['entitycons']

print author_collection.find_one()
print author_collection.find({'PostDate': '20140204',
			'Author': 'ShattahSwagg',
			'Connection': 'BeautyArabian',
			'Type': 'Reply',
			'Matching': {

				}
			}).count()

#print 'Total tweets, 1/1/2013 - 1/31/2014: ' + str(author_collection.count())
#temp = author_collection.find({'PostDate': {'$gte': '20130101', '$lte': '20131231'}}).count()
#print 'Total tweets, 1/1/2013 - 12/31/2013: ' + str(temp)
#temp = author_collection.find({'PostDate': {'$gte': '20130101', '$lte': '20130919'}}).count()
#print 'Total tweets, 1/1/2013 - 09/19/2013: ' + str(temp)
#print len(list(author_collection.distinct('Author')))
#print len(list(author_collection.distinct('PostID')))
