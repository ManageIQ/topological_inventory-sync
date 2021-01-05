require "json"
require "base64"
require "manageiq-messaging"
require "topological_inventory/sync/logging"
require "topological_inventory/sync/api_client"

module TopologicalInventory
  class Sync
    class Worker
      include Logging
      include ApiClient

      TOPOLOGY_APP_NAME = "/insights/platform/topological-inventory".freeze

      def initialize(messaging_host, messaging_port, metrics = nil)
        self.messaging_client = nil
        self.messaging_host   = messaging_host
        self.messaging_port   = messaging_port
        self.metrics          = metrics
      end

      def run
        logger.info("Starting #{worker_name} for #{queue_name}...")

        initial_sync

        self.messaging_client = ManageIQ::Messaging::Client.open(messaging_client_opts)
        messaging_client.subscribe_topic(subscribe_opts) do |message|
          begin
            perform(message)
          rescue => err
            metrics&.record_error
            logger.error("#{err.message}\n#{err.backtrace.join("\n")}")
          end
        end
      ensure
        messaging_client&.close
      end

      private

      attr_accessor :messaging_client, :messaging_host, :messaging_port, :metrics, :queue_name

      def initial_sync
        # Override this in your subclass if there is any sync needed to be done
        # prior to blocking on the queue topic for new work aka sync anything
        # that was missed while shutdown
      end

      def perform(message)
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def queue_name
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def persist_ref
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def worker_name
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end

      def messaging_client_opts
        {
          :protocol   => :Kafka,
          :host       => messaging_host,
          :port       => messaging_port,
          :client_ref => "topological-inventory-sync-#{queue_name}"
        }
      end

      def subscribe_opts
        {
          :persist_ref     => persist_ref,
          :service         => queue_name,
          :session_timeout => 60 #seconds
        }
      end
    end
  end
end
