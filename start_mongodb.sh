#!/bin/bash

# An example start script for logstash using Solr output plugin.
java -jar logstash-1.3.3-flatjar.jar agent -f lw_solr_mongodb.conf -p .
