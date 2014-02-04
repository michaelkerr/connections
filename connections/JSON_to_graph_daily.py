#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# JSON to graph, daily #
#################
# Created Date: 2014/01/28
# Last Updated: 2014/02/03

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
#graph_db.clear()


## Classes ###


### Functions ###
def add_authors():
	return


def is_rel_present(existing_rels, rel_props):
	## >Default = relationship does not exist
	is_present = False

	## >Iterate through the nodes with the same PostID
	for entry in existing_rels:
		## >Get the properties of the existing relationship
		entry_props = entry.get_properties()
		## >If the dictionaries are the same, a relationship exists
		if len(set(entry_props.items()) & set(rel_props.items())) > 0:
			is_present = True
	return is_present


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

## >Get the starting numbers: nodes, relationships
nodes_start = graph_db.order
rels_start = graph_db.size

## >Add a root node if one does not exist
#TODO Abstract this for multiple instances, how do we pass this?
instance_name = "Coral"
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")

if (len(node_index.get("name", instance_name)) == 0):
	instance_node, = graph_db.create({"name": instance_name, "type": "instance_root"})
	print 'Created instance node: ' + instance_name
else:
	instance_node = node_index.get("name", instance_name)[0]

## >Get the data from mongodb added 'today'
objectid_r = recent_object_id()
con_docs = list(author_collection.find({"_id": {"$gte": ObjectId(objectid_r)}}))

## >Number of connections to be processed
num_processed = len(con_docs)
print 'Processing ' + str(num_processed) + ' connections'

## >Get the node index from the graphdb
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")

## >Add networks to the graphdb, if not in graph
## >Get the list of networks from mongodb
mongo_networks = list(author_collection.distinct('Network'))
## >Get the list of networks from graphdb
graph_networks = []
for g_network in node_index.get("type", 'Network'):
	graph_networks.append(g_network['name'])
## >Add the nodes if not in the graph
for m_network in mongo_networks:
	## >If the network is not in the graph, add it
	if m_network not in graph_networks:
		print 'Adding network: ' + m_network
		graph_db.create({"name": m_network, "type": "Network"})
		graph_networks.append(m_network)

## >Get a list of projects from the graphdb
graph_projects = []
for project in node_index.get("type", 'Project'):
	graph_projects.append(project['id'])

## >Get a list of topics from the graphdb
graph_topics = []
for topic in node_index.get("type", 'Topic'):
	graph_topics.append(topic['id'])

## >For each connection document
for entry in con_docs:
	matched_projects = []
	matched_topics = []
	scored_projects = []
	scored_topics = []
	match_pro_top = []
	scored_pro_top = []

	## >Get the nodes and relationships in the graph
	node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
	relationship_index = graph_db.get_index(neo4j.Node, "relationship_auto_index")

	## >Create Project and Topic nodes, if they dont exist; Create the Project-->Topic relationship
	if 'Matching' in entry.keys():
		for item in entry['Matching']:
			## >Check if the project name is already in the graph
			if item['ProjectId'] not in graph_projects:
				print 'Adding project: ' + item['ProjectId']
				matched_project_node, = graph_db.create({"id": item['ProjectName'], "name": item['ProjectName'], "type": "Project"})
				graph_projects.append(item['ProjectId'])
				matched_projects.append(item['ProjectId'])
			## >Get the project node
			else:
				matched_project_node, = node_index.get("id", item['ProjectId'])

			## >If there are any topics, check if they are already in the graph
			if len(item['Topics']) > 0:
				for temp_topic in item['Topics']:
					if temp_topic['TopicId'] not in graph_topics:
						print 'Adding topic: ' + temp_topic['TopicId']
						matched_topic_node, = graph_db.create({"id": temp_topic['TopicId'],
											"name": temp_topic['TopicName'],
											"type": "Topic"})
						graph_topics.append(temp_topic['TopicId'])
						matched_topics.append(temp_topic['TopicId'])

						## >Create the Project-->Topic relationship
						print 'Adding Project-->Topic relationship: ' + matched_project_node['id'] + '--includes-->' + matched_topic_node['id']
						graph_db.create((matched_project_node, "includes", matched_topic_node))

	if 'Scoring' in entry.keys():
		for item in entry['Scoring']:
			## >Check if the project name is already in the graph
			if item['ProjectId'] not in graph_projects:
				print 'Adding project: ' + item['ProjectId']
				scored_project_node, = graph_db.create({"id": item['ProjectId'], "name": item['ProjectName'], "type": "Project"})
				graph_projects.append(item['ProjectId'])
				scored_projects.append(item['ProjectId'])
			## >Get the project node
			else:
				scored_project_node, = node_index.get("id", item['ProjectId'])

			## >If there are any topcis, check if they are already in the graph
			if len(item['Topics']) > 0:
				if item['TopicId'] not in graph_topics:
					print 'Adding topic: ' + item['TopicId']
					scored_topic_node, = graph_db.create({"id": item['TopicId'], "name": item['TopicName'], "type": "Topic"})
					graph_topics.append(item['TopicId'])
					scored_topics.append(item['TopicId'])

					## >Create the Project-->Topic relationship
					print 'Adding Project-->Topic relationship: ' + scored_project_node['id'] + '--includes-->' + scored_topic_node['id']
					graph_db.create((scored_project_node, "includes", scored_topic_node))

	## >Create the author nodes, if they dont exist
	#TODO Turn this into a function
	entry_author = None
	entry_connection = None

	## >Get the list of author nodes
	author_node_list = node_index.get("name", entry['Author'])
	if (len(author_node_list) == 0):
		## >If there is no author by that name in the index, create the node
		print "Adding Author Node: " + entry['Author'] + ' to ' + entry['Network']
		entry_author, = graph_db.create({"name": entry['Author'], "type": "Author", "Network": entry['Network']})
	else:
		## >There is at least one author by that name, check if unique
		found_node = None
		for author_node in author_node_list:
			if author_node["Network"] == entry['Network']:
				## >The node exists
				entry_author = author_node
				found_node = 1
				#print 'Author Node "' + entry['Author'] + '" exists. Skipping'
		if found_node != 1:
			entry_author, = graph_db.create({"name": entry['Author'], "type": "Author", "Network": entry['Network']})
			print ">Adding Author Node: " + entry['Author'] + ' to ' + entry['Network']

	## >Create the connection (author) nodes, if they dont exist
	#TODO Turn this into a function
	node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
	author_node_list = node_index.get("name", entry['Connection'])
	if (len(author_node_list) == 0):
		## >If there is no author by that name in the index, create the node
		print "Adding Author Node: " + entry['Connection'] + ' to ' + entry['Network']
		entry_connection, = graph_db.create({"name": entry['Connection'], "type": "Author", "Network": entry['Network']})
	else:
		## >There is at least one author by that name, check if unique
		found_node = None
		for author_node in author_node_list:
			if author_node["Network"] == entry['Network']:
				## >The node exists
				entry_connection = author_node
				found_node = 1
				#print 'Author Node "' + entry['Connection'] + '" exists. Skipping'
		if found_node != 1:
			entry_connection, = graph_db.create({"name": entry['Connection'], "type": "Author", "Network": entry['Network']})
			print ">Adding Author Node: " + entry['Connection'] + ' to ' + entry['Network']

	## >Get the updated index of all nodes and relationships
	node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
	#TODO is this necessary?
	relationship_index = graph_db.get_index(neo4j.Node, "relationship_auto_index")

	## >Get the Network nodes
	network_node = node_index.get("name", entry['Network'])[0]

	## >Get the Topic nodes
	matched_nodes = []
	scored_nodes = []
	for matched in matched_topics:
		matched_nodes.append(node_index.get("name", matched)[0])
	for scored in scored_topics:
		scored_nodes.append(node_index.get("name", scored)[0])
	project_nodes = matched_nodes + scored_nodes

	## >If the Network-->Project relationship doesnt exist, add it
	#TODO speed this up
	for project in project_nodes:
		if len(list(graph_db.match(start_node=network_node, end_node=project))) == 0:
			print "Adding Network-->Project relationship: " + project['name'] + ' --> ' + entry['Network']
			## >Create a new Network-->Project relationship
			graph_db.create((network_node, "belongs_to", project))

	## >If the Network-->Author relationship doesnt exist
	if len(list(graph_db.match(start_node=network_node, end_node=entry_author))) == 0:
		print "Adding N--> A relationship: " + entry['Network'] + ' --> ' + entry['Author']
		## >Create a new Author-->Network relationship
		graph_db.create((network_node, "contains", entry_author))

	## >If the Network-->Connection(Author) relationship doesnt exist
	if len(list(graph_db.match(start_node=network_node, end_node=entry_connection))) == 0:
		print "Adding N--> C(A) relationship: " + entry['Network'] + ' --> ' + entry['Connection']
		## >Create a new Connection(Author)-->Network relationship
		graph_db.create((network_node, "contains", entry_connection))

	## >Create the Author-->Connection relationship for matched and scored
	#TODO Turn this into a function
	if len(matched_nodes) > 0:
		rel_prop_dict = {"name": entry['PostID'],
			"date": entry['PostDate'],
			"time": entry['PostTime'],
			"type": entry['Type'],
			"subforum": None}

		if entry['Network'] != 'twitter':
			rel_prop_dict['subforum'] = entry['Subforum']

		## >Create the Author-->Connection relationship for each matched project
		for topic in matched_nodes:
			rel_prop_dict['matched'] = topic
			#TODO add project
			if is_rel_present(relationship_index.get("name", entry['PostID']), rel_prop_dict) is False:
				graph_db.create((entry_author, "talks_to", entry_connection, rel_prop_dict))
				#print 'Adding Author-->Connection relationship: ' + entry['Author'] + ' --> ' + entry['Connection']
			else:
				print 'Relationship exists: ' + entry['Author'] + ' --> ' + entry['Connection']

			## >If there is no exiting relationship, create one
			if is_rel_present(relationship_index.get("name", entry['PostID']), rel_prop_dict) is False:
				graph_db.create((entry_author, "talks_to", entry_connection, rel_prop_dict))
				#print 'Adding Author-->Connection relationship: ' + entry['Author'] + ' --> ' + entry['Connection']
			else:
				print 'Relationship exists: ' + entry['Author'] + ' --> ' + entry['Connection']

	#TODO Turn this into a function
	if len(scored_nodes) > 0:
		rel_prop_dict = {"name": entry['PostID'],
			"date": entry['PostDate'],
			"time": entry['PostTime'],
			"type": entry['Type'],
			"subforum": None}

		if entry['Network'] != 'twitter':
			rel_prop_dict['subforum'] = entry['Subforum']

		## >Create the Author-->Connection relationship
		for topic in scored_nodes:
			rel_prop_dict['scored'] = topic
			#TODO add project
			## >If there is no exiting relationship, create one
			if is_rel_present(relationship_index.get("name", entry['PostID']), rel_prop_dict) is False:
				graph_db.create((entry_author, "talks_to", entry_connection, rel_prop_dict))
				#print 'Adding Author-->Connection relationship: ' + entry['Author'] + ' --> ' + entry['Connection']
			else:
				print 'Relationship exists: ' + entry['Author'] + ' --> ' + entry['Connection']

## >Get the end numbers: nodes, relationships
nodes_end = graph_db.order
rels_end = graph_db.size

## >Validation
print 'Mongo Authors: ' + str(len(list(author_collection.distinct('Author'))))
print 'Mongo Connections: ' + str(len(list(author_collection.distinct('Connection'))))
print 'Graph Authors: ' + str(len(node_index.get('type', 'Author')))
print 'Start nodes: ' + str(nodes_start)
print 'End nodes: ' + str(nodes_end)
print 'Start Rels: ' + str(rels_start)
print 'End Rels: ' + str(rels_end)
print 'Added ' + str(nodes_end - nodes_start) + ' nodes'
print 'Added ' + str(rels_end - rels_start) + ' rels'

## >Print the time related stats
#TODO log this
end_time = datetime.datetime.now()
script_stats = end_time - start_time
print "Script completed in: " + str(script_stats)