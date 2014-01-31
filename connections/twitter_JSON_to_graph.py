#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# Twitter project specific conenctions update #
#################
# Created Date: 2014/01/28
# Last Updated: 2014/01/28

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
graph_db = neo4j.GraphDatabaseService(server_url)

## >Clear the Graph, nodes and rels
#TODO REMOVE THIS FOR PRODUCTION
graph_db.clear()

## >Add a root node if one does not exist
#TODO Abstract this for multiple instances, how do we pass this?
instance_name = "Coral"
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
if (len(node_index.get("name", instance_name)) == 0):
	instance_node, = graph_db.create({"name": instance_name, "type": "instance_root"})
	print 'Created instance node: ' + instance_name
else:
	instance_node = node_index.get("name", instance_name)[0]

## >Get a list of projects from the mongodb
#TODO
mongo_projects = []

## >Get a list of projects from the graphdb
graph_projects = list(graph_db.match(start_node=instance_node))

## >Add projects not in the graphdb
if len(mongo_projects) > len(graph_projects):
	print 'There are new projects'

## >Get a list of networks from mongodb
network_list = list(author_collection.distinct('Network'))

## >Get the 'new' JSON data
#TODO get 'new'
#TODO storedate/time of last completed filefrom before?
#TODO process everything that has been added "today"?

json_data =

## >Get the starting numbers for the nodes and relationships
nodes_start = graph_db.order
rels_start = graph_db.size

## >Get the node index
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")

## >For each entry in the JSON data
## >Create project nodes if they dont exist

for entry in project_list:
	## >If the project isnt in the index
	#TODO Batch This
	if (len(node_index.get("name", entry)) == 0):
		new_project_node, = graph_db.create({"name": entry, "type": "project"})
	else:
		new_project_node, = node_index.get("name", entry)
	## >Create a relationship to the instance root node, if it doesnt exist
	if len(list(graph_db.match(start_node=instance_node, end_node=new_project_node))) == 0:
		graph_db.create((instance_node, "links", new_project_node))

## >Create Network nodes if they dont exist
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
for entry in network_list:
	## >If the project isnt in the index
	#TODO Batch This
	if (len(node_index.get("name", entry)) == 0):
		graph_db.create({"name": entry, "type": "network"})

## >For each entry in the JSON data
for entry in json_data:
	node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
	entry_author = None
	entry_connection = None

	## >Create the author node, if it doesnt exist
	#TODO create function create_unique_node()
	author_node_list = node_index.get("name", entry['Author'])
	if (len(author_node_list) == 0):
		## >If there is no author by that name in the index, create the node
		entry_author, = graph_db.create({"name": entry['Author'], "type": "author", "network": entry['Network']})
		print "Adding Author Node: " + entry['Author'] + ' to ' + entry['Network']
	else:
		## >There is at least one author by that name, check if unique
		found_node = None
		for author_node in author_node_list:
			if author_node["network"] == entry['Network']:
				## >The node exists
				entry_author = author_node
				found_node = 1
				#print 'Node "' + entry['Author'] + '" exists. Skipping'
		if found_node != 1:
			entry_author, = graph_db.create({"name": entry['Author'], "type": "author", "network": entry['Network']})
			print ">Adding Author Node: " + entry['Author'] + ' to ' + entry['Network']

	## >Create the author (connection) node, if it doesnt exist
	#TODO create function create_unique_node()
	node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
	author_node_list = node_index.get("name", entry['Connection'])
	if (len(author_node_list) == 0):
		## >If there is no author by that name in the index, create the node
		entry_connection, = graph_db.create({"name": entry['Connection'], "type": "author", "network": entry['Network']})
		print "Adding Author(C) Node: " + entry['Connection'] + ' to ' + entry['Network']
	else:
		## >There is at least one author by that name, check if unique
		found_node = None
		for author_node in author_node_list:
			if author_node["network"] == entry['Network']:
				## >The node exists
				entry_connection = author_node
				found_node = 1
				#print 'Node "' + entry['Connection'] + '" exists. Skipping'
		if found_node != 1:
			entry_connection, = graph_db.create({"name": entry['Connection'], "type": "author", "network": entry['Network']})
			print ">Adding Author(C) Node: " + entry['Connection'] + ' to ' + entry['Network']

	## >Get the updated index of all nodes
	node_index = graph_db.get_index(neo4j.Node, "node_auto_index")

	##> Get the Network and Project nodes
	network_node = node_index.get("name", entry['Network'])[0]
	project_node = node_index.get("name", entry['Project'])[0]

	## >If the Network-->Project relationship doesnt exist
	#TODO speed this up
	if len(list(graph_db.match(start_node=network_node, end_node=project_node))) == 0:
		print "Adding N-->P relationship: " + entry['Project'] + ' --> ' + entry['Network']
		## >Create a new Network-->Project relationship
		graph_db.create((network_node, "belongs_to", project_node))

	## >If the Network-->Author relationship doesnt exist
	if len(list(graph_db.match(start_node=network_node, end_node=entry_author))) == 0:
		print "Adding N--> A relationship: " + entry['Network'] + ' --> ' + entry['Author']
		## >Create a new Author-->Network relationship
		graph_db.create((network_node, "contains", entry_author))

	## >If the Connection(Author)-->Network relationship doesnt exist
	if len(list(graph_db.match(start_node=network_node, end_node=entry_connection))) == 0:
		print "Adding N--> C(A) relationship: " + entry['Network'] + ' --> ' + entry['Connection']
		## >Create a new Connection(Author)-->Network relationship
		graph_db.create((network_node, "contains", entry_connection))

	## >Create the Author-->Connection relationship
	rel_prop_dict = {"name": entry['PostID'],
			"date": entry['PostDate'],
			"time": entry['PostTime'],
			"type": entry['Type'],
			"subforum": entry['Subforum'],
			"scored_project": entry['Project'],
			"scored_topic": entry['Topic']}

	#TODO check if the relationship already exists before adding
	graph_db.create((entry_author, "talks_to", entry_connection, rel_prop_dict))
	print "Adding Author-->Connection relationship: " + entry['Author'] + ' --> ' + entry['Connection']

## >Print the number of nodes in the graph
#TODO log this
nodes_final = graph_db.order
rels_final = graph_db.size
print str(graph_db.order) + " nodes in the graph. >> Added " + str(nodes_final - nodes_start) + " nodes"
print str(graph_db.size) + " relationships in the graph. >> Added " + str(rels_final - rels_start) + " relationships"

## >Print the time related stats
#TODO log this
end_time = datetime.now()
stats = end_time - start_time
print "File completed in: " + str(stats)

## >Print the time related stats
#TODO log this
script_end_time = datetime.now()
script_stats = script_end_time - script_start_time
print "Script completed in: " + str(script_stats)