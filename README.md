MongoDBLogstashPlugin
=====================
This plugin provides a MongoDB input for logstash. It can read a whole existing DB and/or tail the oplog in order to get modifications. 

Version 1.0.0
Tested with logstash-1.4.1 and MongoDB 2.6.1
Using MongoDB Ruby driver 1.10.1


Installation instructions
=========================
Download and untar MongoDBLogstashPlugin-1.0.0.tar in the root directory of your logstash installation. 


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


