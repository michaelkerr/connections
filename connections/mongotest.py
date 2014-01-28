#!/usr/bin/python2.7.3
import json
from pymongo import MongoClient

client = MongoClient('192.168.1.152', 27017)

db = client['connections']

collection = db['authorcons']
data_dict = {}

data_dict['PostID'] = '425058271846617100'
data_dict['PostDate'] = '20130228'
data_dict['PostTime'] = '231600'
data_dict['Network'] = 'twitter'
data_dict['Author'] = '399232274'
data_dict['Connection'] = '472232993'
data_dict['Type'] = 'Mention'
data_dict['Scoring'] = {}
data_dict['Scoring']['Scored'] = {}
data_dict['Scoring']['Irrelevant'] = {}
data_dict['Matching'] = {}
data_dict['Meta'] = {}
data_dict['Meta']['Processed'] = '20140127084327865077'
data_dict['Meta']['Guid'] = 'd064e76f-04a5-41b9-95e1-e9954b05d7c3'

post_id = collection.insert(data_dict)

print collection.count()
print collection.find_one()

collection.remove()

print collection.find_one()
