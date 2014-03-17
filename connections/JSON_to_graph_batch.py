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
def add_authors():
	return


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
total_processed = 0

## >Get date range
start_date = datetime.date(2014, 2, 1)
end_date = datetime.date(2014, 2, 14)
day = datetime.timedelta(days=1)
daterange = lambda d1, d2: (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))

## >Get the starting numbers: nodes, relationships
nodes_start = graph_db.order
rels_start = graph_db.size

## >Add a root node if one does not exist
#TODO Abstract this for multiple instances, how do we pass this?
#instance_name = "Coral"
#node_index = graph_db.get_index(neo4j.Node, "node_auto_index")

#if (len(node_index.get("name", instance_name)) == 0):
	#instance_node, = graph_db.create({"name": instance_name, "type": "instance_root"})
	#print 'Created instance node: ' + instance_name
#else:
	#instance_node = node_index.get("name", instance_name)[0]

## >For each day in the time period
for doc_date in daterange(start_date, end_date):
	print 'Processing ' + str(doc_date) + ' for graphdb connection updates'

	## >Create an objectid for the start date
	start_objectid = daily_object_id(doc_date)
	## >Create an objectid for the end date
	end_objectid = daily_object_id(doc_date + datetime.timedelta(days=1))

	## >Get the documents that are in between those date by objectid
	con_docs = list(author_collection.find({
					'_id': {'$gte': ObjectId(start_objectid), '$lt': ObjectId(end_objectid)},
					'Network': 'twitter.com'}
					))

	## >If there are any new connections
	if len(con_docs) > 0:
		## >Number of connections to be processed
		num_processed = len(con_docs)
		print 'Processing ' + str(num_processed) + ' connections'
		i = 1

		#TODO utilize the daily script instead of copy/paste
		## >Data mapping
		graph_network_dict = {}
		graph_project_dict = {}
		graph_topic_dict = {}
		top_proj_dict = {}

		## >Get the starting numbers: nodes, relationships
		doc_nodes_start = graph_db.order
		doc_rels_start = graph_db.size

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
		## >Add the networks if not in the graph
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
			matched_project_map = {}
			matched_topic_map = {}
			scored_project_map = {}
			scored_topic_map = {}
			doc_projects = {}

			## >Get the nodes and relationships in the graph
			node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
			relationship_index = graph_db.get_index(neo4j.Relationship, "relationship_auto_index")

			#####################
			## >Create Project and Topic nodes, if they dont exist; Create the Project-->Topic relationship
			if 'Matching' in entry.keys():
				for item in entry['Matching']:
					## >Check if the project name is already in the graph
					if item['ProjectId'] not in graph_project_dict.keys():
						print 'Adding project: ' + item['ProjectId']
						## >Create the node
						matched_project_node, = graph_db.create({"id": item['ProjectId'], "name": item['ProjectName'], "type": "Project"})
						## >Add to the graph dict, document dict
						graph_project_dict[matched_project_node['name']] = matched_project_node
					else:
						## >Get the project node
						matched_project_node, = node_index.get("id", item['ProjectId'])
					if matched_project_node['id'] not in matched_project_map.keys():
						matched_project_map[matched_project_node['id']] = matched_project_node
					if matched_project_node['id'] not in doc_projects.keys():
						doc_projects[matched_project_node['id']] = matched_project_node

					## >If there are any topics, check if they are already in the graph
					if len(item['Topics']) > 0:
						for temp_topic in item['Topics']:
							if temp_topic['TopicId'] not in graph_topic_dict.keys():
								print 'Adding topic: ' + temp_topic['TopicId']
								matched_topic_node, = graph_db.create({"id": temp_topic['TopicId'],
													"name": temp_topic['TopicName'],
													"type": "Topic"})
								graph_topic_dict[temp_topic] = matched_topic_node

								## >Create the Project-->Topic relationship
								print 'Adding Project-->Topic relationship: ' + matched_project_node['id'] + '--includes-->' + matched_topic_node['id']
								graph_db.create((matched_project_node, "includes", matched_topic_node))
							else:
								matched_topic_node, = node_index.get("id", temp_topic)
							if temp_topic not in top_proj_dict.keys():
								top_proj_dict[temp_topic] = item['ProjectId']
							if temp_topic not in matched_topic_map.keys():
								matched_topic_map[temp_topic] = matched_topic_node

			if 'Scoring' in entry.keys():
				scored_projects = []
				scored_topics = []
				for item in entry['Scoring']:
					## >Check if the project name is already in the graph
					if item['ProjectId'] not in graph_project_dict.keys():
						print 'Adding project: ' + item['ProjectId']
						scored_project_node, = graph_db.create({"id": item['ProjectId'], "name": item['ProjectName'], "type": "Project"})
						graph_project_dict[matched_project_node] = scored_project_node
					## >Get the project node
					else:
						scored_project_node, = node_index.get("id", item['ProjectId'])
					scored_project_map[scored_project_node['id']] = scored_project_node
					doc_projects[scored_project_node['id']] = scored_project_node

					## >If there are any topics, check if they are already in the graph
					if len(item['Topics']) > 0:
						for temp_topic in item['Topics']:
							if item['TopicId'] not in graph_topic_dict.keys():
								print 'Adding topic: ' + item['TopicId']
								scored_topic_node, = graph_db.create({"id": item['TopicId'], "name": item['TopicName'], "type": "Topic"})
								graph_topic_dict[temp_topic] = scored_topic_node

								## >Create the Project-->Topic relationship
								print 'Adding Project-->Topic relationship: ' + scored_project_node['id'] + '--includes-->' + scored_topic_node['id']
								graph_db.create((scored_project_node, "includes", scored_topic_node))
							else:
								scored_topic_node, = node_index.get("id", temp_topic)
							if temp_topic not in top_proj_dict.keys():
								top_proj_dict[temp_topic] = item['ProjectId']
							scored_topic_map[temp_topic] = scored_topic_node

			#####################
			## >Create the author nodes, if they dont exist
			#TODO Turn this into a function
			entry_author = None
			entry_connection = None

			## >Get the list of author nodes
			author_node_list = node_index.get("name", entry['Author'])
			if (len(author_node_list) == 0):
				## >If there is no author by that name in the index, create the node
				#print "Adding Author Node: " + entry['Author'] + ' to ' + entry['Network']
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
					#print ">Adding Author Node: " + entry['Author'] + ' to ' + entry['Network']

			## >Create the connection (author) nodes, if they dont exist
			#TODO Turn this into a function
			node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
			author_node_list = node_index.get("name", entry['Connection'])
			if (len(author_node_list) == 0):
				## >If there is no author by that name in the index, create the node
				#print "Adding Author Node: " + entry['Connection'] + ' to ' + entry['Network']
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
					#print ">Adding Author Node: " + entry['Connection'] + ' to ' + entry['Network']

			## >Get the updated index of all nodes and relationships
			node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
			relationship_index = graph_db.get_index(neo4j.Relationship, "relationship_auto_index")

			## >Get the Network Node
			#TODO get this from graph_network_dict
			network_node = node_index.get("name", entry['Network'])[0]
			#network_node = graph_network_dict[entry['Network']]

			#####################
			## >If the Network-->Project relationship doesnt exist, add it
			#TODO speed this up
			#TODO check the local dict first
			#print doc_projects
			for p_name, p_node in doc_projects.iteritems():
				if len(list(graph_db.match(start_node=network_node, end_node=p_node))) == 0:
					print "Adding Network-->Project relationship: " + p_name + ' --> ' + entry['Network']
					## >Create a new Network-->Project relationship
					graph_db.create((network_node, "belongs_to", p_node))

			#####################
			## >Network-->Author relationships
			#TODO cache locally as well?
			author_list = [entry_author, entry_connection]
			tweet_source_list = []
			## >If Network == twitter, create a list of source
			if entry['Network'] == 'twitter.com':
				if (u'sources' in entry['Meta'].keys()):
					tweet_source_list = []
					tweet_source_dict = {}
					for source in entry['Meta']['sources']:
						tweet_source_dict['source'] = source
						tweet_source_list.append(tweet_source_dict)

			## >Once for the author node, once for the connection
			for author_node in author_list:
				na_rel_list = list(graph_db.match(start_node=network_node, end_node=author_node))
				## >If there is any source info
				if len(tweet_source_list) > 0:
					## >If there are any matching relationships
					if len(na_rel_list) > 0:
						rel_sources = []
						for na_rel in na_rel_list:
							## >Get the sources in the graph for this rel
							rel_sources.append(na_rel['source'])
						for tweet_source in tweet_source_list:
							if tweet_source['source'] not in rel_sources:
								print "Adding N--> A relationship: " + network_node['name'] + ' --> ' + author_node['name'] + ', source = ' + tweet_source['source']
								graph_db.create((network_node, "contains", author_node, tweet_source))
					else:
						## >There are none, go ahead and add all source
						for tweet_source in tweet_source_list:
							print 'Adding N--> A relationship: ' + network_node['name'] + ' --> ' + author_node['name'] + ', source = ' + tweet_source['source']
							## >Create a new Author-->Network relationship
							graph_db.create((network_node, "contains", author_node, tweet_source))

				## >Or there is no source info, just check and create if none
				elif len(list(graph_db.match(start_node=network_node, end_node=author_node))) == 0:
						print "Adding N--> A relationship: " + network_node['name'] + ' --> ' + author_node['name']
						## >Create a new Author-->Network relationship
						graph_db.create((network_node, "contains", author_node))
				else:
					print 'Error creating Network-->Author reationship'
					#TODO log this

			#####################
			## >Create the Author-->Connection relationship for matched and scored
			#TODO Turn this into a function
			#TODO check a relationship exists before adding
			## >Build the connection
			#TODO
			new_con_list = []
			rel_prop_dict = {"name": entry['PostID'],
				"date": entry['PostDate'],
				"time": entry['PostTime'],
				"type": entry['Type'],
				"subforum": None}

			if entry['Network'] != 'twitter.com':
				rel_prop_dict['subforum'] = entry['Subforum']

			## >Encode the project related info
			rel_prop_dict['matched_project'] = str(tuple(matched_project_map.keys())).encode('base64', 'strict')
			rel_prop_dict['matched_topic'] = str(tuple(matched_topic_map.keys())).encode('base64', 'strict')
			rel_prop_dict['scored_project'] = str(tuple(scored_project_map.keys())).encode('base64', 'strict')
			rel_prop_dict['scored_topic'] = str(tuple(scored_topic_map.keys())).encode('base64', 'strict')

			## >Get all relationships from author to connection
			auth_con_rels = relationship_index.get('name', entry['PostID'])
			#auth_con_rels = graph_db.match(start_node=entry_author,
							#rel_type="talks_to",
							#end_node=entry_connection,
							#limit=None,
							#bidirectional=False)
			if len(auth_con_rels) == 0:
				print 'Adding A-->A relationship: ' + entry_author['name'] + ' --> ' + entry_connection['name']
				graph_db.create((entry_author, "talks_to", entry_connection, rel_prop_dict))

			print 'Completed ' + str(i) + ' of ' + str(num_processed) + ' connections.'
			i += 1

		## >Get the end numbers: nodes, relationships
		doc_nodes_end = graph_db.order
		doc_rels_end = graph_db.size
		print 'Added ' + str(doc_nodes_end - doc_nodes_start) + ' nodes'
		print 'Added ' + str(doc_rels_end - doc_rels_start) + ' rels'
			#TODO LOG EACH DAYS STATS

## >Get the end numbers: nodes, relationships
nodes_end = graph_db.order
rels_end = graph_db.size

## >Validation
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
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