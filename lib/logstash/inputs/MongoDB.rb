
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
  config :sync_mode, :validate => [ "full", "stored", "force" ], :required => true

  # Read preference?
  config :read, :validate => [ "primary", "primary_preferred", "secondary", "secondary_preferred", "nearest" ], :default => "primary"

  # How often should we save the oplog read point
  config :oplog_sync_flush_inteval, :validate => :number, :default => 10

  # Where to save the oplog read point
  config :oplog_sync_file, :validate => :string, :default => "oplogSync.json"

  # Max retry before exiting on connection failure. Use -1 for never
  config :max_retry, :validate => :number, :default => 10



  public
  def register
    require 'mongo'
    @uriParsed=Mongo::URIParser.new(@uri)
    setReadPreference
  end # def register


  private
  def connect
    @logger.warn("Connecting to MongoDB at #{@uriParsed.node_strings}")
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
  def readOplog(host, coll, output_queue)
    
    query = {'fromMigrate' => {'$exists' => false}}
    
    if !@oplogSyncPoint[host].nil? && !@sync_mode.eql?("force") 
      ts = BSON::Timestamp.new(@oplogSyncPoint[host]['seconds'], @oplogSyncPoint[host]['increment'])
      query['ts'] = {'$gt' => ts}
    end
    
    if !@collection.eql?("*")
      query['ns'] = "#{@db_name}.#{@collection}"
    end      
    
    @logger.debug("Using query #{query} for oplog at #{host}")

    cursor = coll.find(query, :hint => '$natural')
    cursor.add_option(Mongo::Constants::OP_QUERY_TAILABLE)

    loop do
      if doc = cursor.next_document
        @oplogSyncPoint[host] = {'seconds' => doc['ts'].seconds, 'increment' => doc['ts'].increment}
        @retryCounter = 0
        output_queue << doc
      else
        sleep 1
      end
    end
  end # def readOplog


  private 
  def sync(output_queue)
    if File.exist?(@oplog_sync_file)
      File.open( @oplog_sync_file, "r" ) do |f|
        @oplogSyncPoint = JSON.load(f)
        f.close
      end
    else
      @logger.info("Oplog sync file #{@oplog_sync_file} does not exist.")
      @oplogSyncPoint = {}
    end


    #Creating a new thread which will record every 'oplog_sync_flush_inteval' seconds the last operation read for each replicaset
    oplogSync = Thread.new {
      while true
        sleep @oplog_sync_flush_inteval
        
        File.open( @oplog_sync_file, "w" ) do |f|
          JSON.dump(@oplogSyncPoint, f)
          f.close
        end
      end  
    }


    if @mongoClient.mongos? #This is a sharded cluster
      threads = []
      threadCounter = 0

      configDb = @mongoClient.db("config") # Need to check the config DB 

      
      configDb.collection("shards").find.each do |shardDoc|
        threads[threadCounter] = Thread.new { 
          # Transforms "shard0/xxx:27100,yyy:27101,zzz:27102" in "mongodb://xxx:27100,yyy:27101,zzz:27102/local"
          seedList = shardDoc["host"].match(/\/(.*)/)[1]
          @logger.info("Connecting to oplog using mongodb://#{seedList}/local")
          
          @retryCounter = 0
          syncSharded("mongodb://#{seedList}/local", output_queue) 

        } # Parrallelized the job
        threadCounter += 1  
      end

      
      threads.each {|t| t.join} # Wait for all threads to be finished  
    else # Not a sharded cluster 
      syncNonSharded(output_queue) 
    end
  end # def sync

  private
  def syncSharded(host, output_queue) 
    coll = nil

    begin
      mongoLocalClient = Mongo::URIParser.new(host).connection({})
      db = mongoLocalClient.db()
      coll = db.collection("oplog.rs", :read => @read_preference)
    rescue => e
      @logger.error("Can't connect to oplog")
      raise e
    end

    begin
      readOplog(host, coll, output_queue) 
    rescue Mongo::ConnectionFailure
      if @max_retry >= 0 && @retryCounter >= @max_retry
        @logger.error("Max retry attempt reached limit of #{@max_retry}. Exiting now")
        exit
      else
        @retryCounter += 1
        @logger.warn("Connection to MongoDB had a problem. Attempt #{@retryCounter} in 10 seconds")
        sleep 10
        syncSharded(host, output_queue) 
      end
    rescue => e 
      @logger.error(e)
    end
  end

  private
  def syncNonSharded(output_queue) 
    localDb = @mongoClient.db("local")
    coll = localDb.collection("oplog.rs", :read => @read_preference)
    begin
      readOplog(@uriParsed.node_strings, coll, output_queue) 
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
        if File.exist?(@oplog_sync_file)
          @logger.warn("Oplog sync file #{@oplog_sync_file} was found. Use sync mode 'force' to resync completly.")
        else
          readStoredData(output_queue)
        end
      end

      if @sync_mode.eql?("force")  #We need to read the data already stored in the DB and discard oplog sync file
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