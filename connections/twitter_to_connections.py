# -*- coding: utf-8 -*-
# Twitter to connections #
#################
# Created Date: 2014/01/21
# Last Updated: 2014/01/22

### Resources ###
import json
import MySQLdb
import datetime

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

### Classes ###


### Functions ###
def yesterday():
	weekday_dict = {1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'}
	month_dict = {
			'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
			'07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
			}
	m_d_y = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
	mdy_list = str(m_d_y).split('-')
	return (weekday_dict[datetime.date.isoweekday(m_d_y)] + ' ' + month_dict[mdy_list[1]] + ' ' + mdy_list[2])


def get_tweet_date(twitter_date_time):
	#"PostDate": "20130228",
	#"PostTime": "231600",
	#Thu Oct 31 09:48:36 +0000 2013
	month_rev_dict = {
				'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
				'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
			}
	date_time_list = twitter_date_time.split(' ')
	tweet_date = date_time_list[5] + month_rev_dict[date_time_list[1]] + date_time_list[2]
	tweet_time = date_time_list[3].replace(':', '')
	return tweet_date, tweet_time


### Main ###
start_time = datetime.datetime.now()

connections_data = []

cur = db.cursor()

## >Get all "New" Tweets
#TODO  -determine efficient way to get "new"
#TODO
## >Get todays date, format as Twitter date/time
#TODO - remove, testing only
#tweet_date = 'Mon Jan 20'

## >Get the date for 'yesterday'
tweet_date = yesterday()

query = 'SELECT * FROM raw_tweets WHERE tweet_body LIKE "%' + tweet_date + '%";'
cur.execute(query)
tweet_data = cur.fetchall()

## >If there are any new tweets
if len(tweet_data) > 0:
	for tweet in tweet_data:
		data_dict = {}

		## >Turn the tweet body back into a JSON dict
		tweet_body = json.loads(tweet[2])

		## >Get the tweet id
		data_dict['PostID'] = tweet_body['id_str']

		## >Get the tweet date/time
		data_dict['PostDate'], data_dict['PostTime'] = get_tweet_date(tweet_body['created_at'])

		data_dict['Network'] = 'twitter'

		## >Get the tweet author
		data_dict['Author'] = tweet_body['user']['id_str']

		## >Get the tweet project(s)
		data_dict['Project'] = tweet_body['meta']['topics']

	## >Discover Mentions
	# if 'user_mentions'
	#For each project....
	#data_dict['Type'] = 'mention'

	## >Discover Replies
	#in_reply_to_user_id
	#data_dict['Type'] = ''

	## >Add "New" to "Active" Tweets

	## >Get all "Active" Tweets
		print data_dict

print str(datetime.datetime.now() - start_time)