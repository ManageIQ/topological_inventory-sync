require "rest-client"
require "topological_inventory/sync/worker"
require "uri"

module TopologicalInventory
  class Sync
    class SourcesSyncWorker < Worker
      include Logging

      def internal_tenant
        "system_orchestrator"
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
          supported_applications_by_source_id = supported_applications(tenant).group_by(&:source_id)

          sources         = sources_api_client(tenant).list_sources.data
          enabled_sources = sources.select { |source| supported_applications_by_source_id[source.id].present? }

          enabled_sources.each do |source|
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
        when "Application.create"
          source = sources_api_client(tenant).show_source(payload["source_id"].to_s)

          Source.create!(
            :id     => source.id,
            :uid    => source.uid,
            :tenant => tenants_by_external_tenant(tenant),
          )
        when "Application.destroy"
          source = Source.find_by(:id => payload["source_id"])
          source&.destroy
        when "Source.destroy"
          source = Source.find_by(:uid => payload["uid"])
          source&.destroy
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

      def supported_applications(tenant)
        sources_api_client(tenant).list_applications.data
          .select { |app| supported_application_type_ids.include?(app.application_type_id) }
      end

      def supported_application_type_ids
        @supported_application_type_ids ||= begin
          supported_application_names = ["/insights/platform/catalog", "/insights/platform/topological-inventory"]

          sources_api_client(internal_tenant).list_application_types.data
            .select { |at| supported_application_names.include?(at.name) }.map(&:id)
        end
      end

      def internal_tenants_url
        config = SourcesApiClient.configure
        URI::HTTP.build(
          :host   => config.host.split(":").first,
          :port   => config.host.split(":").last,
          :path   => "/internal/v1.0/tenants"
        ).to_s
      end

      def headers_to_account_number(headers)
        JSON.parse(Base64.decode64(headers["x-rh-identity"])).dig("identity", "account_number")
      end
    end
  end
end
