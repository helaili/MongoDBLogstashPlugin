
# encoding: utf-8
require "logstash/inputs/base"
require "logstash/inputs/threadable"
require "logstash/namespace"

#
# This input plugin will read data from a user collection in a MongoDB database and/or listen to updates in the oplog
#

class LogStash::Inputs::MongoDB < LogStash::Inputs::Threadable
  config_name "MongoDB"
  milestone 1

  default :codec, "json"

  # The URI of your MongoDB server or replica set.
  config :uri, :validate => :string, :default => "mongodb://localhost:27017/test"

  # The name of the collection
  config :collection, :validate => :string, :required => true

  # What kind of data do we want
  config :sync_mode, :validate => [ "full", "new", "stored" ], :required => true

  # Read preference?
  config :read, :validate => [ "primary", "primary_preferred", "secondary", "secondary_preferred", "nearest" ], :default => "primary"

  # Read preference?
  config :auth_db, :validate => :string, :default => "admin"

  public
  def register
    require 'mongo'
    @logger.info("Registering MongoDB, mongoClientecting to #{@uri}")
    @uriParsed=Mongo::URIParser.new(@uri)
    setReadPreference
  end # def register


  private
  def connect
    @logger.warn("Connecting to MongoDB at #{@uri}")
  	@mongoClient = @uriParsed.connection({})
    
    
    return @mongoClient.db()
  end # def connect

  private 
  def setReadPreference
    if @read.eql?("primary")
      @read_preference = :primary  
    elsif @read.eql?("primary_preferred")
      @read_preference = :primary_preferred
    elsif @read.eql?("secondary")
      @read_preference = :secondary
    elsif @read.eql?("secondary_preferred")
      @read_preference = :secondary_preferred
    elsif @read.eql?("nearest")
      @read_preference = :nearest
    end 

  end #setReadPreference

  private
  def readCollection(collName, output_queue)
    collection = @db.collection(collName,  :read => @read_preference)
    namespace = "#{collection.db.name}.#{collection.name} "
    
    @logger.warn("Reading MongoDB collection #{namespace}")
    
    
    collection.find.each do
      |row|
      row["ns"] = namespace
      output_queue << row
    end
  end # def readCollection


  private 
  def readStoredData(output_queue)
    if @collection.eql?("*") # We want all the collections of this DB
      threads = []
      threadCounter = 0

      @db.collection_names.each do |collName|
        threads[threadCounter] = Thread.new{ readCollection(collName, output_queue) } # Parrallelized the job
        threadCounter += 1
      end 

      threads.each {|t| t.join} # Wait for all threads to be finished
    else  # Only the collection specified in the config file has to be processed
      readCollection(@collection, output_queue)
    end
  end # def readStoredData


  private 
  def readOplog(coll, output_queue)
    if !@ts.nil?
      @logger.warn(@ts)
    end
        
    #cursor = Mongo::Cursor.new(coll, :tailable => true, :order => [['$natural', 1]])
    if @collection.eql?("*") # We want all the collections of this DB
      if @ts.nil?
        cursor = coll.find({'fromMigrate' => {'$exists' => false}}, :hint => '$natural')
      else
        cursor = coll.find({'fromMigrate' => {'$exists' => false}, ts: {'$gte' => @ts}}, :hint => '$natural')
      end
    else
      if @ts.nil?
        cursor = coll.find({'fromMigrate' => {'$exists' => false}, 'ns' => "#{@db_name}.#{@collection}"}, :hint => '$natural')
      else 
        cursor = coll.find({'fromMigrate' => {'$exists' => false}, 'ns' => "#{@db_name}.#{@collection}", ts: {'$gte' => @ts}}, :hint => '$natural')
      end
    end
    cursor.add_option(Mongo::Constants::OP_QUERY_TAILABLE)

    loop do
      if doc = cursor.next_document
        @ts = doc['ts']
        output_queue << doc
      else
        sleep 1
      end
    end
  end # def readOplog


  private 
  def sync(output_queue)
    if @mongoClient.mongos? #This is a sharded cluster
      threads = []
      threadCounter = 0

      configDb = @mongoClient.db("config") # Need to check the config DB 

      authOn = false
      username = nil
      password = nil
      authSource = nil
        

      @uriParsed.auths.each do |auth|
        username = auth[:username]
        password = auth[:password]
        authSource = auth[:source]
        authOn = true
      end
      
      configDb.collection("shards").find.each do |shardDoc|
        threads[threadCounter] = Thread.new { 
          # Transforms "shard0/xxx:27100,yyy:27101,zzz:27102" in "mongodb://xxx:27100,yyy:27101,zzz:27102/local"
          seedList = shardDoc["host"].match(/\/(.*)/)[1]
          @logger.info("Connecting to oplog using mongodb://#{seedList}/local")
          
          @ts = nil
          
          #if authOn
          #  syncSharded("mongodb://#{username}:#{password}@#{seedList}/local?authSource=#{authSource}", output_queue) 
          #else 
          #  syncSharded("mongodb://#{seedList}/local", output_queue) 
          #end
          syncSharded("mongodb://#{seedList}/local", output_queue) 

        } # Parrallelized the job
        threadCounter += 1  
      end

      threads.each {|t| t.join} # Wait for all threads to be finished  
    else # Not a sharded cluster 
      @ts = nil
      syncNonSharded(output_queue) 
    end
  end # def sync

  private
  def syncSharded(host, output_queue) 
    @logger.warn("xxxxxxxxxxxx #{host} xxxxxxxxxxxx")
    mongoLocalClient = Mongo::URIParser.new(host).connection({})
    db = mongoLocalClient.db()
    coll = db.collection("oplog.rs", :read => @read_preference)
    begin
      readOplog(coll, output_queue) 
    rescue
      @logger.warn("Connection to MongoDB had a problem. Reconnecting")
      syncSharded(host, output_queue) 
    end
  end

  private
  def syncNonSharded(output_queue) 
    localDb = @mongoClient.db("local")
    coll = localDb.collection("oplog.rs", :read => @read_preference)
    begin
      readOplog(coll, output_queue) 
    rescue
      @logger.warn("Connection to MongoDB had a problem. Reconnecting")
      syncNonSharded(output_queue) 
    end
  end



  public
  def run(output_queue)
    begin
      @db ||= connect
      @db_name = @db.name

  
      if @sync_mode.eql?("full") || @sync_mode.eql?("stored") #We need to read the data already stored in the DB
        readStoredData(output_queue)
      end

      @db = nil

      if not @sync_mode.eql?("stored") #We need to read the oplog 
        sync(output_queue)
      end
    rescue => e # MongoDB error
      @logger.warn("Failed to get data from MongoDB", :exception => e, :backtrace => e.backtrace)
      raise e
    end
  end # def run

  public
  def teardown

  end
end # class LogStash::Inputs::MongoDB