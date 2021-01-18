require "rest-client"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/iterator"
require "topological_inventory/sync/metrics/sources_sync"
require "more_core_extensions/core_ext/module/cache_with_timeout"
require "uri"

module TopologicalInventory
  class Sync
    class SourcesSyncWorker < Worker
      include Logging

      attr_reader :source_uids_by_id

      def initialize(messaging_host, messaging_port, metrics)
        @source_uids_by_id = {}
        super
      end

      def worker_name
        "Topological Inventory Sync Worker"
      end

      def queue_name
        "platform.sources.event-stream"
      end

      def persist_ref
        "topological-inventory-sync-sources"
      end

      def self.application_types(tenant)
        block = ->(limit, offset) { sources_api_client(tenant).list_application_types(:limit => limit, :offset => offset) }
        TopologicalInventory::Sync::Iterator.new(block)
      end

      def applications(tenant)
        block = ->(limit, offset) { sources_api_client(tenant).list_applications(:limit => limit, :offset => offset) }
        TopologicalInventory::Sync::Iterator.new(block)
      end

      def sources(tenant)
        block = ->(limit, offset) { sources_api_client(tenant).list_sources(:limit => limit, :offset => offset) }
        TopologicalInventory::Sync::Iterator.new(block)
      end

      def source_applications(tenant, source_id)
        block = ->(resource_id, limit, offset) { sources_api_client(tenant).list_source_applications(resource_id, :limit => limit, :offset => offset) }
        TopologicalInventory::Sync::Iterator.new(block, :resource_id => source_id.to_s)
      end

      def initial_sync
        sources_by_uid       = {}
        tenant_by_source_uid = {}

        tenants.each do |tenant|
          supported_applications = []
          applications(tenant).each do |app|
            supported_applications << app if supported_application_type_ids.include?(app.application_type_id)
          end

          supported_applications_by_source_id = supported_applications.group_by(&:source_id)

          sources(tenant).each do |source|
            next if supported_applications_by_source_id[source.id].blank?

            sources_by_uid[source.uid]       = source
            tenant_by_source_uid[source.uid] = tenant
          end
        end

        current_source_uids  = sources_by_uid.keys
        previous_source_uids = Source.pluck(:uid)

        sources_to_delete = previous_source_uids - current_source_uids
        sources_to_create = current_source_uids - previous_source_uids

        logger.info("Deleting sources [#{sources_to_delete.join("\n")}]") if sources_to_delete.any?
        sources_arel = Source.where(:uid => sources_to_delete)
        cnt = sources_arel.count
        sources_arel.destroy_all
        metrics&.source_destroyed(cnt) unless cnt.zero?

        sources_to_create.each do |source_uid|
          logger.info("Creating source [#{source_uid}]")

          source = sources_by_uid[source_uid]
          tenant = tenants_by_external_tenant(tenant_by_source_uid[source_uid])

          Source.create!(
            :id     => source.id,
            :tenant => tenant,
            :uid    => source_uid
          )
          metrics&.source_created(1)
        end

        logger.info("Initial sync completed...")
      rescue => err
        logger.error("Full sync: #{err.message}\n#{err.backtrace.join("\n")}")
        metrics&.record_error(:full_sync)
      end

      def perform(message)
        jobtype = message.message
        payload = message.payload

        logger.info("#{jobtype}: #{payload}")

        tenant = headers_to_account_number(message.headers)

        case jobtype
        when "Source.create"
          source_uids_by_id[payload["id"]] = payload["uid"]
        when "Application.create"
          source_id, application_type_id = payload.values_at("source_id", "application_type_id")

          if supported_application_type_ids.include?(application_type_id.to_s)
            source_uid = source_uids_by_id[source_id] || sources_api_client(tenant).show_source(source_id.to_s)&.uid
            create_source(source_id, source_uid, tenant)
          end
        when "Application.destroy"
          source_id = payload["source_id"]
          source_apps = []
          source_applications(tenant, source_id.to_s).each { |source_app| source_apps << source_app }
          source_application_type_ids = source_apps.collect(&:application_type_id).uniq
          # Delete Source if there is no remaining supported application
          if (source_application_type_ids & supported_application_type_ids).blank?
            destroy_source(source_id)
          end
        when "Source.destroy"
          source_uids_by_id.delete(payload["id"])
          destroy_source(payload["id"])
        end
      end

      def create_source(id, uid, tenant)
        source = Source.find_by(:id => id, :uid => uid)
        if source.blank?
          Source.create!(:id => id, :uid => uid, :tenant => tenants_by_external_tenant(tenant))
          metrics&.source_created(1)
        end
      rescue => e
        logger.error("Failed to create Source (id: #{id}, uid: #{uid}). Error: #{e.message} | #{e.cause}")
      end

      def destroy_source(id)
        source = Source.find_by(:id => id)
        if source.present?
          source.destroy
          metrics&.source_destroyed(1)
        end
      end

      def tenants_by_external_tenant(external_tenant)
        @tenants_by_external_tenant ||= {}
        @tenants_by_external_tenant[external_tenant] ||= Tenant.find_or_create_by(:external_tenant => external_tenant)
      end

      # TODO: Pagination not available in Sources Internal API!
      def tenants
        response = RestClient.get(internal_tenants_url, identity_headers("topological_inventory-sources_sync"))
        JSON.parse(response).map { |tenant| tenant["external_tenant"] }
      end

      cache_with_timeout(:supported_application_type_ids) do
        application_types = []
        application_types("system_orchestrator").each { |app_type| application_types << app_type }
        application_types.select { |application_type| needs_topology?(application_type) }.map(&:id)
      end

      def supported_application_type_ids
        self.class.supported_application_type_ids
      end

      def self.needs_topology?(application_type)
        application_type.name == TOPOLOGY_APP_NAME || application_type.dependent_applications.include?(TOPOLOGY_APP_NAME)
      end

      def internal_tenants_url
        config = SourcesApiClient.configure
        host, port = config.host.split(":")
        URI::HTTP.build(:host => host, :port => port || 443, :path => "/internal/v1.0/tenants").to_s
      end

      def headers_to_account_number(headers)
        JSON.parse(Base64.decode64(headers["x-rh-identity"])).dig("identity", "account_number")
      end
    end
  end
end
