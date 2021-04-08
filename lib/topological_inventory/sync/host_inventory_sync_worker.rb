require "concurrent"
require 'rest_client'
require "uri"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/application_metrics"
require "topological_inventory-ingress_api-client"
require "topological_inventory/providers/common/save_inventory/saver"
require "topological_inventory/sync/configuration"
require "topological_inventory/sync/host_inventory_response_worker"
require "topological_inventory/sync/clowder_config"

module TopologicalInventory
  class Sync
    class HostInventorySyncWorker < Worker
      include Logging

      TOPOLOGICAL_INVENTORY_API_VERSION = "v3.0".freeze
      HOST_INVENTORY_REPORTER           = "topological-inventory".freeze
      STALE_TIMESTAMP_OFFSET            = 86400 # 1 day from now (seconds)

      attr_reader :source, :sns_topic

      def initialize(topological_inventory_api_params_hash, host_inventory_api, queue_host, queue_port, metrics)
        super(queue_host, queue_port)
        self.topological_inventory_api_params_hash = topological_inventory_api_params_hash
        self.host_inventory_api                    = host_inventory_api
        self.metrics                               = metrics
        self.response_config                       = TopologicalInventory::Sync::Configuration.new(:queue_host => queue_host, :queue_port => queue_port)
        self.response_worker                       = TopologicalInventory::Sync::ResponseWorker.new(response_config, logger, metrics)
      end

      def worker_name
        "Topological Inventory Host Inventory Sync Worker"
      end

      def queue_name
        "platform.topological-inventory.persister-output-stream"
      end

      def persist_ref
        "topological-inventory-host-inventory-sync"
      end

      def run
        response_worker.start
        super
      ensure
        response_worker.stop
        metrics&.stop_server
      end

      def initial_sync
        # noop
      end

      def build_topological_inventory_url_from(additional_params)
        additional_params[:path] = File.join(topological_inventory_api_params_hash[:path], additional_params[:path]) if additional_params[:path]
        URI::HTTP.build(topological_inventory_api_params_hash.merge(additional_params)).to_s
      end

      class << self
        def strip_scheme(host)
          host.to_s.sub(/\Ahttps?:\/\//, '')
        end

        def build_topological_inventory_ingress_url(host, port)
          URI::HTTP.build(
            :host => strip_scheme(host),
            :port => port
          )
        end

        def build_topological_inventory_url_hash(host, port, path_prefix, app_name)
          path = File.join("/", path_prefix.to_s, app_name.to_s, TOPOLOGICAL_INVENTORY_API_VERSION)
          {:host => strip_scheme(host), :port => port, :path => path}
        end

        def build_host_inventory_url(host, path_prefix)
          path = File.join("/", path_prefix.to_s, "inventory", "v1")

          u      = URI(host)
          u.path = path
          u.to_s
        end
      end

      private

      attr_accessor :log, :topological_inventory_api_params_hash, :host_inventory_api, :metrics, :response_config, :response_worker

      def perform(message)
        payload        = message.payload
        account_number = payload["external_tenant"]
        source         = payload["source"]
        x_rh_identity  = Base64.encode64({"identity" => {"account_number" => account_number}}.to_json)

        unless payload["external_tenant"]
          logger.error("Skipping payload because of missing :external_tenant. Payload: #{payload}")
          metrics&.record_error(:bad_request)
          return
        end

        return if (vms = extract_vms(payload)).empty?

        logger.info("#{vms.size} VMs found for account_number: #{account_number} and source: #{source}.")

        logger.info("Fetching #{vms.size} Topological Inventory VMs from API.")

        topological_inventory_vms = get_topological_inventory_vms(vms, x_rh_identity)
        logger.info("Fetched #{topological_inventory_vms.size} Topological Inventory VMs from API.")

        logger.info("Syncing #{topological_inventory_vms.size} Topological Inventory VMs with Host Inventory")

        stale_timestamp = (Time.now.utc + STALE_TIMESTAMP_OFFSET).to_datetime

        topological_inventory_vms.each do |host|
          # Skip processing if we've already created this host in Host Based
          next if !host["host_inventory_uuid"].nil? && !host["host_inventory_uuid"].empty?
          # Only send RHEL hosts to HBI
          next unless rhel?(host)
          # HBI requires MAC address, so hosts without MAC address are not sent
          next if host["mac_addresses"].to_a.empty?

          data = {
            :display_name    => host["name"],
            :external_id     => host["source_ref"],
            :mac_addresses   => host["mac_addresses"],
            :account         => account_number,
            :reporter        => HOST_INVENTORY_REPORTER,
            :stale_timestamp => stale_timestamp,
            :system_profile  => {:is_marketplace => marketplace?(host)}
          }

          # TODO(lsmola) use the possibility to create batch of VMs in Host Inventory
          create_host_inventory_hosts(data, source)
        end
      rescue => e
        logger.error("#{e.message} -  #{e.backtrace.join("\n")}")
        metrics&.record_error(:request)
      end

      def rhel?(host)
        host["guest_info"] == "Red Hat BYOL Linux" ||
          host["guest_info"] == "Red Hat Enterprise Linux"
      end

      def marketplace?(host)
        host["guest_info"] != "Red Hat BYOL Linux"
      end

      def create_host_inventory_hosts(data, source)
        client = ManageIQ::Messaging::Client.open(:protocol => :Kafka, :host => messaging_host, :port => messaging_port)
        client.publish_message(
          :service => TopologicalInventory::Sync::ClowderConfig.kafka_topic('platform.inventory.host-ingress'),
          :message => 'update_host_inventory',
          :payload => {
            "operation" => "add_host",
            "data"      => data
          }.to_json
        )
        response_worker.register_message(data[:external_id], source)
      rescue => err
        logger.error("Exception in `create_host_inventory_hosts`: #{err}\n#{err.backtrace.join("\n")}")
        metrics&.record_error(:request)
      ensure
        client&.close
      end

      def get_host_inventory_hosts(x_rh_identity)
        RestClient::Request.execute(
          :method  => :get,
          :url     => File.join(host_inventory_api, "hosts"),
          :headers => {"x-rh-identity" => x_rh_identity}
        )
      end

      def extract_vms(payload)
        vms         = payload.dig("payload", "vms") || {}
        changed_vms = (vms["updated"] || []).map { |x| x["id"] }
        created_vms = (vms["created"] || []).map { |x| x["id"] }
        deleted_vms = (vms["deleted"] || []).map { |x| x["id"] }

        (changed_vms + created_vms + deleted_vms).compact
      end

      # TODO: Use API client
      def get_topological_inventory_vms(vms, x_rh_identity)
        return [] if vms.empty?

        uri       = build_topological_inventory_url_from(:path => 'vms', :query => {:filter => {:id => vms}}.to_query)
        found_vms = JSON.parse(RestClient::Request.execute(:method => :get, :url => uri.to_s, :headers => {"x-rh-identity" => x_rh_identity}).body)['data']

        missing_vms_ids = vms - found_vms.map { |x| x['id'].to_i }
        if missing_vms_ids.present?
          logger.info("Vms #{missing_vms_ids.inspect} were not found in Topological Inventory")
        end

        found_vms
      end
    end
  end
end
