#!/usr/bin/python2.7.3
# -*- coding: utf-8 -*-
# Twitter to connections, daily #
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

#TODO read config file

db = MySQLdb.connect(host="ingest.cudb3djsmyrc.us-west-2.rds.amazonaws.com",
			user="",
			passwd="",
			db="ingestdb")

mongoclient = MongoClient('XXX.XXX.XXX.XXX', 27017)
mongo_db = mongoclient['connections']
entity_collection = mongo_db['entitycons']


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
entity_collection.remove()

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
		entity_list = []


			#################################################
			## >Discover Entity Connections
			## >Secondary, non-author->author connections
			#TODO language

			## >Discover Hashtags
			if 'entities' in tweet_body.keys():
				## >If there are any hashtags
				if len(tweet_body['entities']['hashtags']) > 0:
					for hashtag in tweet_body['entities']['hashtags']:
						hashtag_dict = update_ent_dict(data_dict, hashtag['text'], 'Hashtag')
						if hashtag_dict not in entity_list:
							entity_list.append(hashtag_dict)

			## >Discover urls
			## >From Content
			if 'entities' in tweet_body.keys():
				## >If there are any hashtags
				if len(tweet_body['entities']['urls']) > 0:
					for url in tweet_body['entities']['urls']:
						url_dict = update_ent_dict(data_dict, url['expanded_url'], 'Link')
						url_dict['Meta']['UrlType'] = 'Content'
						if url_dict not in entity_list:
							entity_list.append(url_dict)

			## >From user URLs
			if 'entities' in tweet_body['user'].keys():
				if 'url' in tweet_body['user']['entities'].keys():
					for url in tweet_body['user']['entities']['url']['urls']:
						url_dict = update_ent_dict(data_dict, url['expanded_url'], 'Link')
						url_dict['Meta']['UrlType'] = 'User'
						if url_dict not in entity_list:
							entity_list.append(url_dict)

			## >From user description URLs
			if 'entities' in tweet_body['user'].keys():
				if len(tweet_body['user']['entities']['description']['urls']) > 0:
					for url in tweet_body['user']['entities']['description']['urls']:
						url_dict = update_ent_dict(data_dict, url['expanded_url'], 'Link')
						url_dict['Meta']['UrlType'] = 'Description'
						if url_dict not in entity_list:
							entity_list.append(url_dict)

			## >Discover tweet source
			tweet_source = re.sub(r'<.+?>', '', tweet_body['source'])
			tweet_source = tweet_source.replace('Twitter for ', '').replace('Tweetbot for ', '').strip()
			source_dict = update_ent_dict(data_dict, tweet_source, 'Source')
			if source_dict not in entity_list:
				entity_list.append(source_dict)

			## >Discover Locations
			## >Coordinates
			## >'Geo' is in Twitter JSON but has been depricated, same as Coordinates
			if tweet_body['coordinates'] is not None:
				if tweet_body['coordinates']['type'] is not 'point':
					geo_content = tweet_body['coordinates']
				else:
					geo_content = tweet_body['coordinates']['coordinates']
				geo_dict = update_ent_dict(data_dict, geo_content, 'Location')
				geo_dict['Meta']['LocationType'] = 'Coordinates'
				if geo_dict not in entity_list:
					entity_list.append(source_dict)

			## >
			#TODO add city etc
			if tweet_body['place'] is not None:
				if 'country_code' in tweet_body['place'].keys():
					geo_content = tweet_body['place']['country_code']
				geo_dict = update_ent_dict(data_dict, geo_content, 'Location')
				geo_dict['Meta']['LocationType'] = 'Place'
				geo_dict['Meta']['LocationDesc'] = 'Country'
				if geo_dict not in entity_list:
					entity_list.append(source_dict)

			## >
			if len(tweet_body['user']['location']) > 0:
				pass
				## >Country
				## >City
				## >Region
				# >.......

			## >
			if tweet_body['user']['utc_offset'] is not None:
				geo_dict = update_ent_dict(data_dict, tweet_body['user']['utc_offset'], 'Location')
				geo_dict['Meta']['LocationType'] = 'UTC_Offset'
				if geo_dict not in entity_list:
					entity_list.append(source_dict)

			## >
			if tweet_body['user']['time_zone'] is not None:
				geo_dict = update_ent_dict(data_dict, tweet_body['user']['time_zone'], 'Location')
				geo_dict['Meta']['LocationType'] = 'Timezone'
				if geo_dict not in entity_list:
					entity_list.append(source_dict)

			# >Upload entity connections to mongodb
			for entity in entity_list:
				print 'Adding ' + str(len(entity_list)) + 'Entities'
				if len(list(entity_collection.find({
					'PostTime': entity['PostTime'],
					'PostDate': entity['PostDate'],
					'PostID': entity['PostID'],
					'Network': entity['Network'],
					'Author': entity['Author'],
					'Entity': entity['Entity'],
					'Type': entity['Type']}))) == 0:
						entity_collection.insert(entity)

		except ValueError as e:
			print 'JSON Exception, Logging'
			with open('batch_log.txt', 'a') as output_file:
				output_file.write('> ' + str(datetime.datetime.now()) + '\n')
				output_file.write(str(e) + '\n')
				output_file.write(str(tweet[2]) + '\n')

## >Create logging details
#TODO Add # of items added to each
print entity_collection.count()

print str(datetime.datetime.now() - start_time)
