require "base64"
require "json"
require "rest-client"
require "topological_inventory/sync/worker"
require "uri"

module TopologicalInventory
  class Sync
    class SourcesSyncWorker < Worker
      include Logging

      def queue_name
        "platform.sources.event-stream"
      end

      def persist_ref
        "topological-inventory-sync-sources"
      end

      def initial_sync
        sources_by_uid = tenants.flat_map do |tenant|
          sources_api_client(tenant).list_sources.data
        end.index_by(&:uid)

        current_source_uids  = sources_by_uid.keys
        previous_source_uids = Source.pluck(:uid)

        sources_to_delete = previous_source_uids - current_source_uids
        sources_to_create = current_source_uids - previous_source_uids

        logger.info("Deleting sources [#{sources_to_delete.join("\n")}]") if sources_to_delete.any?
        Source.where(:uid => sources_to_delete).destroy_all

        sources_to_create.each do |source_uid|
          logger.info("Creating source [#{source_uid}]")

          source = sources_by_uid[source_uid]
          tenant = tenants_by_external_tenant(source.tenant)
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

        case jobtype
        when "Source.create"
          Source.create!(
            :id     => payload["id"],
            :uid    => payload["uid"],
            :tenant => tenants_by_external_tenant(payload["tenant"]),
          )
        when "Source.destroy"
          Source.find_by(:uid => payload["uid"]).destroy
        end
      end

      def tenants_by_external_tenant(external_tenant)
        @tenants_by_external_tenant ||= {}
        @tenants_by_external_tenant[external_tenant] ||= Tenant.find_or_create_by(:external_tenant => external_tenant)
      end

      def sources_api_client(tenant = nil)
        api_client = SourcesApiClient::ApiClient.new
        api_client.default_headers.merge!(identity_headers(tenant)) if tenant
        SourcesApiClient::DefaultApi.new(api_client)
      end

      def tenants
        response = RestClient.get(internal_tenants_url, identity_headers("topological_inventory-sources_sync"))
        JSON.parse(response).map { |tenant| tenant["external_tenant"] }
      end

      def internal_tenants_url
        config = SourcesApiClient.configure
        URI::HTTP.build(
          :host   => config.host.split(":").first,
          :port   => config.host.split(":").last,
          :path   => "/internal/v1.0/tenants"
        ).to_s
      end

      def identity_headers(tenant)
        {
          "x-rh-identity" => Base64.strict_encode64(
            JSON.dump({"identity" => {"account_number" => tenant}})
          ),
          "x-rh-insights-request-id" => "1"
        }
      end
    end
  end
end
