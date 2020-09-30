module TopologicalInventory
  class Sync
    class Configuration
      # Kafka message auto-ack (default false)
      attr_accessor :queue_auto_ack
      # Kafka host name
      attr_accessor :queue_host
      # Kafka topic max bytes received in one response
      attr_accessor :queue_max_bytes
      # Kafka topic grouping (if nil, all subscribes receives all messages)
      attr_accessor :queue_persist_ref
      # Kafka port
      attr_accessor :queue_port
      # Kafka topic name for cloud receptor controller's responses
      attr_accessor :queue_topic

      # Timeout for how long successful request waits for response
      attr_accessor :response_timeout
      # Interval between timeout checks
      attr_accessor :response_timeout_poll_time

      def initialize(queue_host: nil, queue_port: nil)
        @pre_shared_key    = nil
        @queue_auto_ack    = false
        @queue_host        = queue_host
        @queue_max_bytes   = nil
        @queue_persist_ref = ENV['HOSTNAME']
        @queue_port        = queue_port
        @queue_topic       = 'platform.inventory.events'

        @response_timeout           = 1.hour
        @response_timeout_poll_time = 1.hour

        yield(self) if block_given?
      end

      def self.default
        @default ||= new
      end

      def configure
        yield(self) if block_given?
      end
    end
  end
end
