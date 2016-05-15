# nuvi_interview
aggregate news data and publish to Redis

# Requirements
This URL (http://bitly.com/nuvi-plz) is an http folder containing a list of zip files. Each zip file contains a bunch of xml files. Each xml file contains 1 news report.

Your application needs to download all of the zip files, extract out the xml files, and publish the content of each xml file to a redis list called “NEWS_XML”.

Make the application idempotent. We want to be able to run it multiple times but not get duplicate data in the redis list.
