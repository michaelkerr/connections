# -*- coding: utf-8 -*-
# IO JSON connections to graphdb #
#################

# Created Date: 2013/10/23
# Last Updated: 2013/12/10

### Resources ###
from datetime import datetime
import json
import os
from py2neo import neo4j

server_url = 'http://XXX.XXX.XXX.XXX:7474/db/data'
#filename = '13-10_SCORED_POSTS_AQ (RU)_9271_W-PostId.xml.txt'
#filename = 'SCORED_POSTS_AQ (A)_2ndQtr-13-W_PostId.xml.txt'

### Classes ###


### Functions ###
def create_unique_node():
	return


def get_con_JSON(in_file):
	#Empty dictionary
	data_dict = {}
	data_list = []
	proj_list, net_list = [], []

	#read data from JSON file
	json_data = json.loads(open(in_file).read())

	for entry in json_data:
		#Post
		try:
			data_dict['PostID'] = entry["PostId"]
		except:
			data_dict['PostID'] = entry["Id"]
		try:
			data_dict['PostDate'] = entry["PostDate"].split("T")[0].replace('-', '')
		except:
			data_dict['PostDate'] = entry["Date"].split("T")[0].replace('-', '')
		try:
			data_dict['PostTime'] = entry["PostDate"].split("T")[1].replace(':', '')
		except:
			data_dict['PostTime'] = entry["Date"].split("T")[1].replace(':', '')
		#Network
		data_dict['Network'] = json.dumps(entry["Domain"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		#>>Author
		data_dict['Author'] = json.dumps(entry["Author"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		#>>Connection
		data_dict['Connection'] = json.dumps(entry["Connection"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		#>> Type
		try:
			data_dict['Type'] = entry["Type"]
		except:
			data_dict['Type'] = "Mention"
		#Subforum
		data_dict['Subforum'] = json.dumps(entry["SubForum"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		#Project
		data_dict['Project'] = json.dumps(entry["Project"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		#Topic
		data_dict['Topic'] = json.dumps(entry["Topic"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		#Tags
		try:
			data_dict['Tags'] = json.dumps(entry["Tags"], ensure_ascii=False, encoding="utf-8").replace('\"', '')
		except:
			data_dict['Tags'] = "None"
		if data_dict['Project'] not in proj_list:
			proj_list.append(data_dict['Project'])
		if data_dict['Network'] not in net_list:
			net_list.append(data_dict['Network'])

		data_list.append(data_dict)
		data_dict = {}
	return data_list, proj_list, net_list


### Main ###
## >Get command line options
#parser = argparse.ArgumentParser(description='Description')

#TODO add logging

## >Get the start time of the script
script_start_time = datetime.now()

## >Get the list of files
local_path = os.path.dirname(os.path.realpath(__file__))
files = os.listdir(local_path)

## >Create link to graph db
graph_db = neo4j.GraphDatabaseService(server_url)

## >Clear the Graph, nodes and rels
#TODO REMOVE THIS
#graph_db.clear()

## >Add a root node if one does not exist
#TODO Abstract this for multiple instances, how do we pass this?
instance_name = "Coral"
node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
if (len(node_index.get("name", instance_name)) == 0):
	instance_node, = graph_db.create({"name": instance_name, "type": "instance_root"})
	print 'Created instance node: ' + instance_name
else:
	instance_node = node_index.get("name", instance_name)[0]

## >For each filename in the filelist
for filename in files:
	if "PostId" in filename:
		print 'Processing ' + filename
		## >Get the start time of the file
		start_time = datetime.now()

		## >Get the data from JSON file
		json_data, project_list, network_list = get_con_JSON(filename)

		## >Get the starting numbers: nodes, relationships
		nodes_start = graph_db.order
		rels_start = graph_db.size

		## >Create project nodes if they dont exist
		node_index = graph_db.get_index(neo4j.Node, "node_auto_index")
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
