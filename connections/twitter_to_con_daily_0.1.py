#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# Twitter to connections #
#################
# Created Date: 2014/01/21
# Last Updated: 2014/01/22

### Resources ###
import datetime
import json
import MySQLdb
from pymongo import MongoClient
import re

#import twitter_A2A as twit_con
#import twitter_entities as twit_ent

weekday_dict = {1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'}

month_dict = {
		'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
		'07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
		}

month_rev_dict = {
		'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
		'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
		}

db = MySQLdb.connect(host="ingest.cudb3djsmyrc.us-west-2.rds.amazonaws.com",
			user="influence",
			passwd="8RiV3wDYV6BWpKRt",
			db="ingestdb")

#mongoclient = MongoClient('192.168.86.140', 27017)
#mongo_db = mongoclient['connections']
#author_collection = mongo_db['authorcons']
#entity_collection = mongo_db['entitycons']

#TODO remove - testing only
#Clear the databases
#author_collection.remove()
#entity_collection.remove()
#Print the current contents of the db

### Classes ###


### Functions ###
def append_to_file(fileout, data_dict):
	with open(fileout, 'a') as output_file:
		output_file.write(json.dumps(data_dict) + ',')
	if not output_file.closed:
		output_file.close()
	return


def yesterday():
	m_d_y = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
	mdy_list = str(m_d_y).split('-')
	return (weekday_dict[datetime.date.isoweekday(m_d_y)] + ' ' + month_dict[mdy_list[1]] + ' ' + mdy_list[2])


def get_tweet_date(twitter_date_time):
	date_time_list = twitter_date_time.split(' ')
	tweet_date = date_time_list[5] + month_rev_dict[date_time_list[1]] + date_time_list[2]
	tweet_time = date_time_list[3].replace(':', '')
	return tweet_date, tweet_time


def start_end_file(fileout, switch):
	with open(fileout, 'a') as output_file:
		if switch == 'start':
			output_file.write('[')
		else:
			output_file.write(']')
	if not output_file.closed:
		output_file.close()
	return


### Main ###
start_time = datetime.datetime.now()

#TODO upload inline, dont create a list first - json.dumps([dict(mpn=pn) for pn in lst])
connection_list = []
entity_list = []
connection_file = 'author_connections.txt'
entity_file = 'entity_connections.txt'

cur = db.cursor()

## >Get all "New" Tweets
#TODO  -determine efficient way to get "new"

#TODO - remove, testing only
## >Get the date for 'yesterday'
tweet_date = yesterday()
tweet_date = "Mon Jan 20"
query = 'SELECT * FROM raw_tweets WHERE tweet_body LIKE "%' + tweet_date + '%";'

## > Twitter raw data connection
cur.execute(query)
tweet_data = cur.fetchall()

## >If there are any new tweets
if len(tweet_data) > 0:
	start_end_file(connection_file, 'start')
	start_end_file(entity_file, 'start')

	for tweet in tweet_data:
		data_dict = {}

		#################################################
		## >Discover Author Connections
		## >Turn the tweet into dict
		tweet_body = json.loads(tweet[2])

		## >Create a unique identifier
		data_dict['ConID'] = str(datetime.datetime.now()).replace('-', '').replace(':', '').replace('.', '').replace(' ', '')

		## >Get the tweet id
		data_dict['PostID'] = tweet_body['id_str']

		## >Get the tweet date/time
		data_dict['PostDate'], data_dict['PostTime'] = get_tweet_date(tweet_body['created_at'])

		## >Set the network
		data_dict['Network'] = 'twitter'

		## >Get the tweet author
		data_dict['Author'] = tweet_body['user']['id_str']

		## >Discover Mentions
		if 'entities' in tweet_body.keys():
			## >If there are any mentions
			if len(tweet_body['entities']['user_mentions']) > 0:
				## >Iterate through the mentions
				for mention in tweet_body['entities']['user_mentions']:
					## >Create the connection
					## >Type = 'mention'
					mention_dict = data_dict
					mention_dict['Connection'] = mention['id_str']
					mention_dict['Type'] = 'Mention'
					#connection_list.append(mention_dict)
					append_to_file(connection_file, mention_dict)

		## >Discover Replies
		if tweet_body['in_reply_to_screen_name'] is not None:
			## >Create the connection
			## >Type = 'reply'
			reply_dict = data_dict
			reply_dict['Connection'] = tweet_body['in_reply_to_screen_name']
			reply_dict['Type'] = 'Reply'
			if len(reply_dict['Connection']) == 0:
				print 'No Reply'
			#connection_list.append(reply_dict)
			append_to_file(connection_file, reply_dict)

		## >Add "New" to "Active" Tweets
		#TODO
		'''
		if tweet_body['retweeted'] is True:
			#TODO add to active retweets

		if tweet_body['favorited'] is True:
		#TODO add to active favorites
		'''
'''
		#################################################
		## >Discover Entity Connections
		#TODO Secondary, non-author->author connections
		#TODO language

		## >Discover Hashtags
		if 'entities' in tweet_body.keys():
			## >If there are any hashtags
			if len(tweet_body['entities']['hashtags']) > 0:
				for hashtag in tweet_body['entities']['hashtags']:
					hashtags_dict = data_dict
					hashtags_dict['Entity'] = hashtag['text']
					hashtags_dict['Type'] = 'Hashtag'
					append_to_file(entity_file, hashtags_dict)

		## >Discover urls
		## >From Content
		if 'entities' in tweet_body.keys():
			## >If there are any hashtags
			if len(tweet_body['entities']['urls']) > 0:
				for url in tweet_body['entities']['urls']:
					#TODO this is shared, merge
					url_dict = data_dict
					url_dict['Entity'] = url['expanded_url']
					url_dict['Type'] = 'Link'
					append_to_file(entity_file, url_dict)

		## >From user URLs
		if 'url' in tweet_body['user']['entities'].keys():
			for url in tweet_body['user']['entities']['url']['urls']:
				#TODO this is shared, merge
				url_dict = data_dict
				url_dict['Entity'] = url['expanded_url']
				url_dict['Type'] = 'Link'
				append_to_file(entity_file, url_dict)

		## >From user description URLs
		if len(tweet_body['user']['entities']['description']['urls']) > 0:
			for url in tweet_body['user']['entities']['description']['urls']:
				#TODO this is shared, merge
				url_dict = data_dict
				url_dict['Entity'] = url['expanded_url']
				url_dict['Type'] = 'Link'
				append_to_file(entity_file, url_dict)

		## >Discover tweet source
		source_dict = data_dict
		tweet_source = re.sub(r'<.+?>', '', tweet_body['source'])
		tweet_source = tweet_source.replace('Twitter for ', '').replace('Tweetbot for ', '').strip()
		source_dict['Entity'] = tweet_source
		source_dict['Type'] = 'Source'
		append_to_file(entity_file, source_dict)

		## >Discover Locations
		## >Coordinates
		## >'Geo' is in Twitter JSON but has been depricated, same as Coordinates
		if tweet_body['coordinates'] is not None:
			geo_dict = data_dict
			if tweet_body['coordinates']['type'] is not 'point':
				geo_dict['Entity'] = tweet_body['coordinates']
			else:
				geo_dict['Entity'] = tweet_body['coordinates']['coordinates']
			geo_dict['Type'] = 'Location'
			append_to_file(entity_file, geo_dict)

		## >
		#TODO store the type of location?
		#TODO revisit
		if tweet_body['place'] is not None:
			if 'country_code' in tweet_body['place'].keys():
				geo_dict = data_dict
				geo_dict['Entity'] = tweet_body['place']['country_code']
				geo_dict['Type'] = 'Location'
				append_to_file(entity_file, geo_dict)
			if 'full_name' in tweet_body['place'].keys():
				if 'city' in tweet_body['place']['place_type']:
					geo_dict = data_dict
					geo_dict['Entity'] = tweet_body['place']['full_name']
					geo_dict['Type'] = 'Location'
					append_to_file(entity_file, geo_dict)
				else:
					pass
					#print tweet_body['place']
			#print tweet_body['place']

		## >
		if len(tweet_body['user']['location']) > 0:
			pass
			## >Country
			## >City
			## >Region
			# >.......

		## >
		if tweet_body['user']['utc_offset'] is not None:
			#TODO normalize in some way?
			geo_dict = data_dict
			geo_dict['Entity'] = tweet_body['user']['utc_offset']
			geo_dict['Type'] = 'Location'
			append_to_file(entity_file, geo_dict)

		## >
		if tweet_body['user']['time_zone'] is not None:
			geo_dict = data_dict
			geo_dict['Entity'] = tweet_body['user']['time_zone']
			geo_dict['Type'] = 'Location'
			append_to_file(entity_file, geo_dict)
'''
	## >Get all "Active" Tweets
	## >Update retweets
	## >Update favorites

start_end_file(connection_file, 'end')
start_end_file(entity_file, 'end')

## >Open the author connections
## >Upload to mongodb
## >Delete the file
#TODO

## >Open the entity connections
#entity_json = json.loads(open(entity_file).read())
#entity_collection.insert(entity_json)
#TODO

print str(datetime.datetime.now() - start_time)