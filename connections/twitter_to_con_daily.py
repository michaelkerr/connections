#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# Twitter to connections, daily #
#################
# Created Date: 2014/01/21
# Last Updated: 2014/02/28

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

#TODO read config file

db = MySQLdb.connect(host="ingest.cudb3djsmyrc.us-west-2.rds.amazonaws.com",
			user="influence",
			passwd="8RiV3wDYV6BWpKRt",
			db="ingestdb")

mongoclient = MongoClient('192.168.1.152', 27017)
mongo_db = mongoclient['connections']
author_collection = mongo_db['authorcons']


### Classes ###


### Functions ###
def add_meta(new_dict, ):
	## >For each list entry add the meta details
	return new_dict


def get_tweet_date(twitter_date_time):
	date_time_list = twitter_date_time.split(' ')
	tweet_date = date_time_list[5] + month_rev_dict[date_time_list[1]] + date_time_list[2]
	tweet_time = date_time_list[3].replace(':', '')
	return tweet_date, tweet_time


def update_con_dict(update_dict, con_type, data_type):
	update_dict['Connection'] = con_type
	update_dict['Type'] = data_type
	return update_dict


def update_ent_dict(update_dict, ent_type, data_type):
	update_dict['Entity'] = ent_type
	update_dict['Type'] = data_type
	return update_dict


def yesterday():
	m_d_y = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
	mdy_list = str(m_d_y).split('-')
	return (weekday_dict[datetime.date.isoweekday(m_d_y)] + ' ' + month_dict[mdy_list[1]] + ' ' + mdy_list[2])


### Main ###
start_time = datetime.datetime.now()

#TODO remove - testing only
#Clear the collections
author_collection.remove()

## >SQL cursor
cur = db.cursor()

## >Get all "New" Tweets
#TODO  -determine efficient way to get "new", since the posts arent guaranteed to be from yesterday (Search API)

#TODO - remove, testing only
## >Get the date for 'yesterday'
tweet_date = yesterday()
query = 'SELECT * FROM raw_tweets WHERE tweet_body LIKE "%' + tweet_date + '%";'

## >Twitter raw data connection
cur.execute(query)
tweet_data = cur.fetchall()

## >If there are any new tweets
if len(tweet_data) > 0:
	print 'Processing ' + str(len(tweet_data)) + ' tweets from ' + str(tweet_date)
	for tweet in tweet_data:
		data_dict = {}
		connection_list = []

		#################################################
		## >Discover Author Connections
		## >Turn the tweet into dict
		try:
			tweet_body = json.loads(tweet[2])

			## >Get the tweet id
			data_dict['PostID'] = tweet_body['id_str']

			## >Get the tweet date/time
			data_dict['PostDate'], data_dict['PostTime'] = get_tweet_date(tweet_body['created_at'])

			## >Set the network
			data_dict['Network'] = 'twitter.com'

			## >Get the tweet author
			data_dict['Author'] = tweet_body['user']['screen_name']

			## >Create the meta dict
			data_dict['Meta'] = tweet_body['meta']
			## >Migrate the topics to the projects
			#TODO remove this when topics are corrected to projects
			## >If todays date earlier than the switchover date
			#TODO
			if 'topics' in data_dict['Meta'].keys():
				if 'projects' not in data_dict['Meta'].keys():
					data_dict['Meta']['projects'] = []
				for topic in data_dict['Meta']['topics']:
					data_dict['Meta']['projects'].append(topic)
				data_dict['Meta']['topics'] = []
			## >Add the twitter id_str to the meta
			data_dict['Meta']['usr_id_str'] = tweet_body['user']['id_str']

			## >Discover Mentions
			if 'entities' in tweet_body.keys():
				## >If there are any mentions
				if len(tweet_body['entities']['user_mentions']) > 0:
					## >Iterate through the mentions
					for mention in tweet_body['entities']['user_mentions']:
						## >Create the connection
						## >Type = 'mention'
						mention_dict = update_con_dict(data_dict, mention['screen_name'], 'Mention')
						if mention_dict not in connection_list:
							connection_list.append(mention_dict)

			## >Discover Replies
			if tweet_body['in_reply_to_screen_name'] is not None:
				## >Create the connection
				## >Type = 'reply'
				reply_dict = update_con_dict(data_dict, tweet_body['in_reply_to_screen_name'], 'Reply')
				if reply_dict not in connection_list:
					connection_list.append(reply_dict)

			## >Add "New" to "Active" Tweets
			#TODO
			if tweet_body['retweeted'] is True:
				pass
				#TODO add to active retweets

			if tweet_body['favorited'] is True:
				pass
				#TODO add to active favorites

			## >Get all "Active" Tweets
			#TODO
			## >Update retweets
			#TODO
			## >Update favorites
			#TODO

			## >Upload author connections to mongodb
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

		except ValueError as e:
			print 'JSON Exception, Logging'
			with open('batch_log.txt', 'a') as output_file:
				output_file.write('> ' + str(datetime.datetime.now()) + '\n')
				output_file.write(str(e) + '\n')
				output_file.write(str(tweet[2]) + '\n')

## >Create logging details
#TODO Add # of items added to each
print author_collection.count()

print str(datetime.datetime.now() - start_time)