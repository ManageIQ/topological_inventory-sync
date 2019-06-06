require "manageiq-messaging"
require "topological_inventory/sync/logging"

module TopologicalInventory
  class Sync
    class Worker
      include Logging

      def initialize(messaging_host, messaging_port)
        self.messaging_client = nil
        self.messaging_host   = messaging_host
        self.messaging_port   = messaging_port
      end

      def run
        logger.info("Starting #{worker_name} for #{queue_name}...")

        initial_sync

        self.messaging_client = ManageIQ::Messaging::Client.open(messaging_client_opts)
        messaging_client.subscribe_topic(subscribe_opts) { |message| perform(message) }
      ensure
        messaging_client&.close
      end

      private

      attr_accessor :messaging_client, :messaging_host, :messaging_port, :queue_name

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

      def messaging_client_opts
        {
          :protocol   => :Kafka,
          :host       => messaging_host,
          :port       => messaging_port,
          :group_ref  => "topological-inventory-sync-#{queue_name}",
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

      def worker_name
        "Topological Inventory Sync Worker"
      end

      def sources_api_client(tenant = nil)
        api_client = SourcesApiClient::ApiClient.new
        api_client.default_headers.merge!(identity_headers(tenant)) if tenant
        SourcesApiClient::DefaultApi.new(api_client)
      end

      def identity_headers(tenant)
        {
          "x-rh-identity" => Base64.strict_encode64(
            JSON.dump({"identity" => {"account_number" => tenant}})
          )
        }
      end
    end
  end
end
