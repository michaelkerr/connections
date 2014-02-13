#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# JSON to graph, daily #
#################
# Created Date: 2014/01/28
# Last Updated: 2014/02/05

### Resources ###
from bson.objectid import ObjectId
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
graph_db = neo4j.GraphDatabaseService(server_url)
## >Clear the Graph, nodes and rels
#TODO REMOVE THIS FOR PRODUCTION
graph_db.clear()


## Classes ###


### Functions ###
def get_or_create_nodes():
	pass


def get_or_create_rels():
	pass


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

### Main ###
start_time = datetime.datetime.now()

## >Data mapping
graph_network_dict = {}
graph_project_dict = {}
graph_topic_dict = {}
top_proj_dict = {}

## >Get the starting numbers: nodes, relationships
nodes_start = graph_db.order
rels_start = graph_db.size

## >Get the data from mongodb added 'today'
objectid_r = recent_object_id()
con_docs = list(author_collection.find({"_id": {"$gte": ObjectId(objectid_r)}}))

## >Number of connections to be processed
num_processed = len(con_docs)
print 'Processing ' + str(num_processed) + ' connections'
i = 0

###################################################################################################
## >Get the node and relationship indexes from the graphdb
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
relationship_index = graph_db.get_index(neo4j.Node, "relationship_auto_index")

#####################
## >Add networks to the graphdb, if not in graph
## >Get the list of networks from mongodb
mongo_networks = list(author_collection.distinct('Network'))
## >Get the list of networks from graphdb
for g_network in node_index.get("type", 'Network'):
	graph_network_dict[g_network['name']] = g_network
## >Add the nodes if not in the graph
for m_network in mongo_networks:
	## >If the network is not in the graph, add it
	if m_network not in graph_network_dict.keys():
		print 'Adding network: ' + m_network
		graph_network_dict[m_network] = graph_db.create({"name": m_network, "type": "Network"})

## >Get a list of projects from the graphdb
for project in node_index.get("type", 'Project'):
	graph_project_dict[project['id']] = project

## >Get a list of topics from the graphdb
for topic in node_index.get("type", 'Topic'):
	graph_topic_dict[topic['id']] = topic
	top_proj_dict[topic['id']] = graph_db.match(start_node=None, end_node=topic, bidirectional=False, limit=None)

###################################################################################################
## >For each connection document
for entry in con_docs:
	pass
	## >Get or create the nodes

	## >Get or create relationships

for entry in con_docs:
	pass