# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# An output that uses Kinesis API.
class LogStash::Outputs::KinesisApi < LogStash::Outputs::Base
  config_name "kinesis_api"

  public
  def register
  end # def register

  public
  def receive(event)
    return "Event received"
  end # def event
end # class LogStash::Outputs::KinesisApi
