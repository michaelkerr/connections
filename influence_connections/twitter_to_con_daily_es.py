#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# Twitter to connections, daily #
#################
# Created Date: 2014/01/21
# Last Updated: 2014/02/28

### Resources ###
from datetime import datetime, date, time, timedelta
import elasticsearch
import json
import logging
from pymongo import MongoClient
import sys
import time as time_2

logging.basicConfig()

weekday_dict = {1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'}

month_dict = {
		'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
		'07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
		}

month_rev_dict = {
		'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
		'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
		}

#TODO read config file
""" Elasticsearch Related information """
es_host = 'search.vendorx.com'
es_port = '9200'
index_name = 'thor'
es_timeout = 30.0
es = elasticsearch.Elasticsearch(host=es_host, port=es_port, timeout=es_timeout)

""" Mongdb related """
mongoclient = MongoClient('192.168.1.152', 27017)
mongo_db = mongoclient['connections']
author_collection = mongo_db['testcons']

### Classes ###


### Functions ###
def add_meta(new_dict, ):
	## >For each list entry add the meta details
	return new_dict


def update_con_dict(update_dict, con_type, data_type):
	update_dict['Connection'] = con_type
	update_dict['Type'] = data_type
	return update_dict


def update_ent_dict(update_dict, ent_type, data_type):
	update_dict['Entity'] = ent_type
	update_dict['Type'] = data_type
	return update_dict


### Main ###
start_time = datetime.now()

# Get all "New" Tweets
# Get the date for 'yesterday'
start_date = (datetime.combine(date.today(), time.min) - timedelta(days=1)).strftime('%a, %d %b %Y %H:%M:%S +0000')
end_date = (datetime.combine(date.today(), time.min) - timedelta(days=1)).strftime('%a, %d %b %Y 23:59:59 +0000')

#start_date = date(2013, 11, 1).strftime('%a, %d %b %Y %H:%M:%S +0000')
#end_date = date(2013, 11, 30).strftime('%a, %d %b %Y %H:%M:%S +0000')

es_query = {"from": 0,
		"size": 1,
		"filtered": {"filter": {"and": [
				{"range": {"interaction.created_at": {"gte": start_date, "lte": end_date}}},
				{"or": [
					{"exists": {"field": "twitter.in_reply_to_screen_name"}},
					{"exists": {"field": "twitter.mentions"}}
					]}
				]
			}
		}
	}

# Query es
es_response = es.search(index=index_name, body={'query': es_query})
# Get number of matching tweets
total_tweets = es_response['hits']['total']

# If there are any new tweets
if total_tweets > 0:
	#TODO LOG THIS
	print 'Processing ' + str(total_tweets) + ' tweets from ' + str(start_date)

	# Set the size to the number of tweets (Get all records)
	es_query['size'] = total_tweets
	es_response = es.search(index=index_name, body={'query': es_query})

	# Extract the tweet data
	tweet_data = es_response['hits']['hits']

	for tweet in tweet_data:
		data_dict = {}
		connection_list = []

		"""
		Author Connections
		"""
		# Turn the tweet into dict
		try:
			tweet_body = tweet['_source']['twitter']
			interaction_content = tweet['_source']['interaction']

			# Get the tweet id
			data_dict['PostID'] = str(tweet_body['id'])

			# Get the tweet date/time
			data_dict['PostDate'] = time_2.strftime('%Y%m%d',
								time_2.strptime(tweet_body['created_at'],
								'%a, %d %b %Y %H:%M:%S +0000')
							)

			data_dict['PostTime'] = time_2.strftime('%H%M%S',
								time_2.strptime(tweet_body['created_at'],
								'%a, %d %b %Y %H:%M:%S +0000')
							)

			# Set the network
			data_dict['Network'] = 'twitter.com'

			# Get the tweet author
			data_dict['Author'] = tweet_body['user']['screen_name'].encode('utf-8')

			# Create the meta dict
			data_dict['Meta'] = {}
			data_dict['Meta']['usr_id_str'] = tweet_body['user']['id_str']

			""" Discover Mentions """
			if 'mentions' in tweet_body.keys():
				for mention in tweet_body['mentions']:
					# Create the connection, Type = 'mention'
					mention_dict = update_con_dict(data_dict, mention.encode('utf-8'), 'Mention')
					if mention_dict not in connection_list:
						connection_list.append(mention_dict)

			""" Discover Replies """
			if 'in_reply_to_screen_name' in tweet_body.keys():
				# Create the connection, Type = 'reply'
				reply_dict = update_con_dict(data_dict, tweet_body['in_reply_to_screen_name'].encode('utf-8'), 'Reply')
				if reply_dict not in connection_list:
					connection_list.append(reply_dict)

			# Add "New" to "Active" Tweets
			#TODO
			#if tweet_body['retweeted'] is True:
				#pass
				#TODO add to active retweets

			#if tweet_body['favorited'] is True:
				#pass
				#TODO add to active favorites

			# Get all "Active" Tweets
			#TODO
			# Update retweets
			#TODO
			# Update favorites
			#TODO

			# Upload author connections to mongodb
			for connection in connection_list:
				if len(list(author_collection.find({
						"PostTime": connection['PostTime'],
						"PostDate": connection['PostDate'],
						'PostID': connection['PostID'],
						'Network': connection['Network'],
						'Author': connection['Author'],
						'Connection': connection['Connection'],
						'Type': connection['Type']}
						))) == 0:
					author_collection.insert(connection)

		except Exception as e:
			print 'Exception, ' + str(e) + ' Logging'
			with open('batch_log.txt', 'a') as output_file:
				output_file.write('> ' + str(e) + '\n')
				output_file.write(str(datetime.now()) + '\n')
				output_file.write(str(e) + '\n')
				output_file.write(str(tweet[2]) + '\n')

# Create logging details
print str(datetime.now() - start_time)