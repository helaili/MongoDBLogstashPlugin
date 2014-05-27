MongoDBLogstashPlugin
=====================
This plugin provides a MongoDB input for logstash. It can read a whole existing DB and/or tail the oplog in order to get modifications. 

Version 1.0.0
Tested with logstash-1.4.1 and MongoDB 2.6.1
Using MongoDB Ruby driver 1.10.1


Installation instructions
=========================
Download and untar MongoDBLogstashPlugin-1.0.0.tar in the root directory of your logstash installation. If you're planning on using SiLK-1.1, do not use its bundle logstash version but rather download a fresh install from the web. 


Integration with SiLK-1.1
=========================
You need a vanilla logstash, not the one that ships with SiLk. Nevertheless, you need to deploy Lucidworks' code to this vanilla install. 
- Copy lucidworks.jar from <SiLK>/solrWriterForLogStash/logstash_deploy to <logstash>/lib, 
- Copy lucidworks_solr_lsv133.rb and lucidworks_solr_lsv122.rb from <SiLK>/solrWriterForLogStash/logstash_deploy/logstash/outputs to <logstash>/lib/logstash/outputs
- Create a new Solr collection yourself or copy the sample one by copying mongodb_collection from <MongoDBLogstashPlugin>/vendor/mongodb/MongoDBLogstashPlugin-1.0.0/solr to <SiLK>/solr-4.7.0/SiLK/solr
- Start Solr
- Start logstash with the right config. Check <MongoDBLogstashPlugin>/vendor/mongodb/MongoDBLogstashPlugin-1.0.0/lw_solr_mongodb.conf for a sample. 



Build
=========================
Just run './build.sh'

In case you want to update the MongoDB driver, you will need to install JRuby and run the following command : 
jruby -S gem install mongo --install-dir <MongoDBLogstashPlugin repo>/vendor/bundle/jruby/1.9



Test
=========================
Well... you know..


Run
=========================
Have a look at the '.conf' and '.sh' files in <MongoDBLogstashPlugin repo>/vendor/mongodb/MongoDBLogstashPlugin-1.0.0


