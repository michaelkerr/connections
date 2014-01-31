#!/usr/bin/python2.7.3

### Resources ###
import datetime
from py2neo import neo4j
from pymongo import MongoClient

## >MongoDB related
mongoclient = MongoClient('192.168.1.152', 27017)
mongo_db = mongoclient['connections']
author_collection = mongo_db['authorcons']
entity_collection = mongo_db['entitycons']

## >GraphDB related
server_url = 'http://192.168.1.164:7474/db/data'


## Classes ###


### Functions ###


### Main ###
start_time = datetime.datetime.now()

## >Create link to graph db
#graph_db = neo4j.GraphDatabaseService(server_url)

## >Clear the Graph, nodes and rels
#TODO REMOVE THIS FOR PRODUCTION
#graph_db.clear()

## >Add a root node if one does not exist
#TODO Abstract this for multiple instances, how do we pass this?
instance_name = "Coral"
#node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
#if (len(node_index.get("name", instance_name)) == 0):
#	instance_node, = graph_db.create({"name": instance_name, "type": "instance_root"})
#	print 'Created instance node: ' + instance_name
#else:
#	instance_node = node_index.get("name", instance_name)[0]

## >Get a list of projects from the graphd
#project_list = list(graph_db.match(start_node=instance_node))

## >Get a list of networks from mongo
network_list =  list(author_collection.distinct('Network'))

for entry in author_collection.find().sort('_id', 1).limit(1):
	print entry['_id'].getTimestamp()