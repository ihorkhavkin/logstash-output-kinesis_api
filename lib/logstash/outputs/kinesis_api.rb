# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require 'aws-sdk-core'

# An output that uses Kinesis API.
class LogStash::Outputs::KinesisApi < LogStash::Outputs::Base
  config_name 'kinesis_api'

  default :codec, 'json'

  # AWS region
  config :region, :validate => :string, :default => 'us-west-2'
  # Kinesis stream
  config :stream_name, :validate => :string, :default => 'development_events_stream'
  # Event keys to use for Kinesis partition key
  config :event_partition_keys, :validate => :array, :default => []

  # AWS region
  # config :region, :validate => :string, :required => true

  public
  def register
    @client = Aws::Kinesis::Client.new(region: @region)
    @codec.on_event(&method(:send_record))
  end # def register

  public
  def receive(event)
    return unless output?(event)

    # Set partition key
    partition_key_parts = ['']
    @event_partition_keys.each do |partition_key_name|
      if not event[partition_key_name].nil? and event[partition_key_name].length > 0
        partition_key_parts << event[partition_key_name].to_s
        break
      end
    end
    event['[@metadata][partition_key]'] = (partition_key_parts * '-').to_s[/.+/m] || '-'

    begin
      @codec.encode(event)
    rescue => e
      @logger.warn('Error encoding event', :exception => e, :event => event)
    end

    # Check if we really need it, but tests seem to check for it
    'Event received'
  end # def event

  def send_record(event, payload)
    begin
      resp = @client.put_record(
          {
              stream_name: @stream_name,
              data: payload,
              partition_key: event['[@metadata][partition_key]'],
          })
      @logger.debug('Put record', :response => resp)
    rescue => e
      @logger.warn('Error writing event to Kinesis', :exception => e)
    end
  end # def send_record

  public
  def close
    @logger.warn('Closed kinesis_api output plugin')
    unless @client.nil?
      @client = nil
    end
  end # def close

end # class LogStash::Outputs::KinesisApi
