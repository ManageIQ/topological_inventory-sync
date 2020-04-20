require "concurrent"
require 'rest_client'
require "uri"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/application_metrics"
require "topological_inventory-ingress_api-client"
require "topological_inventory/providers/common/save_inventory/saver"

module TopologicalInventory
  class Sync
    class HostInventorySyncWorker < Worker
      include Logging

      TOPOLOGICAL_INVENTORY_API_VERSION = "v3.0".freeze
      HOST_INVENTORY_REPORTER           = "topological-inventory".freeze
      STALE_TIMESTAMP_OFFSET            = 86400 # 1 day from now (seconds)

      attr_reader :source, :sns_topic

      def initialize(topological_inventory_api_params_hash, host_inventory_api, queue_host, queue_port, metrics_port)
        super(queue_host, queue_port)
        self.topological_inventory_api_params_hash = topological_inventory_api_params_hash
        self.host_inventory_api                    = host_inventory_api
        self.metrics                               = TopologicalInventory::Sync::ApplicationMetrics.new(metrics_port, "topological_inventory_host_inventory_sync_")
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
        super
      ensure
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

      attr_accessor :log, :topological_inventory_api_params_hash, :host_inventory_api, :metrics

      def perform(message)
        payload        = message.payload
        account_number = payload["external_tenant"]
        source         = payload["source"]
        x_rh_identity  = Base64.encode64({"identity" => {"account_number" => account_number}}.to_json)

        unless payload["external_tenant"]
          logger.error("Skipping payload because of missing :external_tenant. Payload: #{payload}")
          return
        end

        vms = extract_vms(payload)
        return if payload.empty? # No vms were found

        logger.info("#{vms.size} VMs found for account_number: #{account_number} and source: #{source}.")

        logger.info("Fetching #{vms.size} Topological Inventory VMs from API.")

        topological_inventory_vms = get_topological_inventory_vms(vms, x_rh_identity)
        logger.info("Fetched #{topological_inventory_vms.size} Topological Inventory VMs from API.")

        logger.info("Syncing #{topological_inventory_vms.size} Topological Inventory VMs with Host Inventory")

        stale_timestamp = Time.now.utc + STALE_TIMESTAMP_OFFSET

        updated_topological_inventory_vms = []
        topological_inventory_vms.each do |host|
          # Skip processing if we've already created this host in Host Based
          next if !host["host_inventory_uuid"].nil? && !host["host_inventory_uuid"].empty?

          data = {
            :display_name    => host["name"],
            :external_id     => host["source_ref"],
            :mac_addresses   => host["mac_addresses"],
            :account         => account_number,
            :reporter        => HOST_INVENTORY_REPORTER,
            :stale_timestamp => stale_timestamp
          }

          # TODO(lsmola) use the possibility to create batch of VMs in Host Inventory
          created_host = JSON.parse(create_host_inventory_hosts(x_rh_identity, data).body)

          updated_topological_inventory_vms << TopologicalInventoryIngressApiClient::Vm.new(
            :source_ref          => host["source_ref"],
            :host_inventory_uuid => created_host["data"].first["host"]["id"],
          )
        end

        logger.info(
          "Synced #{topological_inventory_vms.size} Topological Inventory VMs with Host Inventory. "\
        "Created: #{updated_topological_inventory_vms.size}, "\
        "Skipped: #{topological_inventory_vms.size - updated_topological_inventory_vms.size}"
        )

        return if updated_topological_inventory_vms.empty?

        logger.info("Updating Topological Inventory with #{updated_topological_inventory_vms.size} VMs.")
        save_vms_to_topological_inventory(updated_topological_inventory_vms, source)
        logger.info("Updated Topological Inventory with #{updated_topological_inventory_vms.size} VMs.")
      rescue => e
        logger.error("#{e.message} -  #{e.backtrace.join("\n")}")
      end

      def save_vms_to_topological_inventory(topological_inventory_vms, source)
        # TODO(lsmola) if VM will have subcollections, this will need to send just partial data, otherwise all subcollections
        # would get deleted. Alternative is having another endpoint than :vms, for doing update only operation. Partial data
        # are not supported without timestamp for update (at least for now), since that is just being added to skeletal
        # precreate.
        TopologicalInventory::Providers::Common::SaveInventory::Saver.new(:client => ingress_api_client, :logger => logger).save(
          :inventory => TopologicalInventoryIngressApiClient::Inventory.new(
            :schema                     => TopologicalInventoryIngressApiClient::Schema.new(:name => "Default"),
            :source                     => source,
            :collections                => [
              TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => :vms, :data => topological_inventory_vms)
            ],
            :refresh_state_part_sent_at => Time.now.utc
          )
        )
      end

      def ingress_api_client
        TopologicalInventoryIngressApiClient::DefaultApi.new
      end

      def create_host_inventory_hosts(x_rh_identity, data)
        RestClient::Request.execute(
          :method  => :post,
          :payload => [data].to_json,
          :url     => File.join(host_inventory_api, "hosts"),
          :headers => {"Content-Type" => "application/json", "x-rh-identity" => x_rh_identity}
        )
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