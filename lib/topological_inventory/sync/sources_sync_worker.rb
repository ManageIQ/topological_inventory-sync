require "rest-client"
require "topological_inventory/sync/worker"
require "more_core_extensions/core_ext/module/cache_with_timeout"
require "uri"

module TopologicalInventory
  class Sync
    class SourcesSyncWorker < Worker
      include Logging

      attr_reader :source_uids_by_id

      def initialize(messaging_host, messaging_port)
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

      def initial_sync
        sources_by_uid       = {}
        tenant_by_source_uid = {}

        tenants.each do |tenant|
          applications            = sources_api_client(tenant).list_applications.data
          supported_applications  = applications.select { |app| supported_application_type_ids.include?(app.application_type_id) }

          supported_applications_by_source_id = supported_applications.group_by(&:source_id)

          sources_api_client(tenant).list_sources.data.each do |source|
            next unless supported_applications_by_source_id[source.id].present?
            sources_by_uid[source.uid]       = source
            tenant_by_source_uid[source.uid] = tenant
          end
        end

        current_source_uids  = sources_by_uid.keys
        previous_source_uids = Source.pluck(:uid)

        sources_to_delete = previous_source_uids - current_source_uids
        sources_to_create = current_source_uids - previous_source_uids

        logger.info("Deleting sources [#{sources_to_delete.join("\n")}]") if sources_to_delete.any?
        Source.where(:uid => sources_to_delete).destroy_all

        sources_to_create.each do |source_uid|
          logger.info("Creating source [#{source_uid}]")

          source = sources_by_uid[source_uid]
          tenant = tenants_by_external_tenant(tenant_by_source_uid[source_uid])

          Source.create!(
            :id     => source.id,
            :tenant => tenant,
            :uid    => source_uid
          )
        end
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
            Source.create!(:id => source_id, :uid => source_uid, :tenant => tenants_by_external_tenant(tenant))
          end
        when "Application.destroy"
          Source.find_by(:id => payload["source_id"])&.destroy
        when "Source.destroy"
          source_uids_by_id.delete(payload["id"])
          Source.find_by(:id => payload["id"])&.destroy
        end
      end

      def tenants_by_external_tenant(external_tenant)
        @tenants_by_external_tenant ||= {}
        @tenants_by_external_tenant[external_tenant] ||= Tenant.find_or_create_by(:external_tenant => external_tenant)
      end

      def tenants
        response = RestClient.get(internal_tenants_url, identity_headers("topological_inventory-sources_sync"))
        JSON.parse(response).map { |tenant| tenant["external_tenant"] }
      end

      cache_with_timeout(:supported_application_type_ids) do
        application_types = sources_api_client("system_orchestrator").list_application_types.data
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
