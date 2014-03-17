import elasticsearch
import logging
from datetime import datetime, date, time, timedelta
import time as time_2
logging.basicConfig()

es_host = 'search.dev.vendorx.com'
es_port = '9200'
index_name = 'thor'
time_out = 30.0
es = elasticsearch.Elasticsearch(host=es_host, port=es_port, timeout=time_out)
# Pagination
page_start = 0
page_size = 100
page_end = page_start + page_size

#""" created 'yesterday' """
start_date = date(2013, 11, 1).strftime('%a, %d %b %Y %H:%M:%S +0000')
end_date = date(2013, 11, 30).strftime('%a, %d %b %Y %H:%M:%S +0000')

es_query = {"from": page_start,
		"size": page_end,
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

es_response = es.search(index=index_name, body={'query': es_query})
print es_response

total_tweets = es_response['hits']['total']

print 'total tweets: ' + str(total_tweets)

if page_end <= total_tweets:
while page_end <= total_tweets:
	print 'start: ' + str(page_start) + ', end: ' + str(page_end) + '\n'
	# QUERY
	page_start = page_end + 1
	page_end = page_start + page_size