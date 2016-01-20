Influence Connections v0.3
==================================================
April 15, 2014


What's new in this release?
--------------------------------------------------
Latest release added support for pulling connections from DataSift format out of the ElasticSearch instance.


What's coming next?
--------------------------------------------------
Considering going to direct query of ES for connections.  Will require work to generate the queries on the fly plus
creating a copy of the GCC data in an ES instance.


What is it?
--------------------------------------------------
Discovers connections in social media data (for now tweets coming via DataSift)
for use in the influence API caclulations/graph.  Also extracts defined entities from
Twitter content (optional)

Consists of several services:
Twitter connections discovery, Daily and Batch (twitter_to_con_daily, twitter_to_con_batch)
Twitter project update (twitter_proj_up)
GCC (Forums/Blogs) connections discovery, Daily and Batch (GCC - Daily, GCC - Batch)
GCC Project matching, Daily and Batch (GCC - Matching)
GCC Project Scoring, Daily and Batch (GCC - Scoring)

Connections Heuristics ('Who is talking to whom')
--------------------------------------------------
[Mention]
If author A mentions author B’s name in a  thread of conversation then A-->B  or (A, B, 1)

[Reply]
If Author A "replies" to Author B then A-->B  or (A, B, 1)
-or- Author A creates a thread (creator or post #1) , author B is the next to post Then B-->A or (B, A, 1)

Not implemented yet:
[Quote]
If Author A quotes Author B, then  A-->B or (A, B, 1)

[Retweet]
If Author A posts a tweet, and Author B re-tweets it, then A-->B.  or (A, B, 1)

[Like]
If Author A "likes", +1 or favorites a post by Author B then B-->A or (B, A, 1)

[Share]
If Author A posts a URL to an article, posts, etc, written by Author B then B-->A or (B, A, 1)


Use
--------------------------------------------------
Runs automatically at 12:01am eastern time (will adjust for daylight savings) via the Microsoft task scheduler.  
Data is available in the 'connections' database and the 'authorcons' collection in the MongoDB.

Documentation
--------------------------------------------------
Minimal documentation is included in the 'docs' folder, more is being made available on the Vendorx Confluence.  Docs folder
includes sample tweets in Twitter JSON and Datasfit formats, plus Connection and Entity connections JSON formats.


Deployment
--------------------------------------------------
Deployment diagram is located in 'docs'.  Connections code is deployed on the same server as the MongoDB.  MongoDB
and connections code are set to autostart on reboot.
