# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

class LogStash::Filters::MongoDBToSolr < LogStash::Filters::Base
  config_name "MongoDBToSolr"
  milestone 1

  
  public
  def register
  end # def register

  public
  def filter(event)
    return unless event.is_a?(LogStash::Event)
    
    event.append({'id' =>  event['_id']}) 


    #if event['op'].eql?('d') 
    #  delete_event = LogStash::Event.new()
    #  delete_event.append({'delete' => {'id' => event['_id']}})
    #  event.overwrite(delete_event)
    #else 
    #  event.append({'id' =>  event['_id']}) 
    #end
  end # def filter


end # class LogStash::Filters::MongoDBToSolr
