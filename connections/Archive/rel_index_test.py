#####
from py2neo import neo4j
server_url = 'http://192.168.1.164:7474/db/data'

#####
## >Create link to graph db
graph_db = neo4j.GraphDatabaseService(server_url)

## >Get the relationship auto index
rel_index = graph_db.get_index(neo4j.Relationship, "relationship_auto_index")
rel_list = rel_index.query("date:{20131001 TO 20131005}")
for entry in rel_list:
	print entry.get_properties()

"""
people = graph_db.get_or_create_index(neo4j.Node, "People")
s_people = people.query("family_name:S*")
"""