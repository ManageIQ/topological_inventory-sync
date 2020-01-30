module TopologicalInventory
  class Sync
    class << self
      attr_writer :topological_api_client
      attr_writer :sources_api_client
    end

    def self.identity_headers(tenant)
      {
        'x-rh-identity' =>
            Base64.strict_encode64(
              JSON.dump(
                'identity' => {
                  'account_number' => tenant,
                  'user'           => {
                    'is_org_admin' => true
                  }
                }
              )
            )
      }
    end

    def self.topological_api_client(tenant = nil)
      @topological_api_client ||= {}
      @topological_api_client[tenant] ||=
        begin
          api_client = TopologicalInventoryApiClient::ApiClient.new
          api_client.default_headers.merge!(identity_headers(tenant)) if tenant
          TopologicalInventoryApiClient::DefaultApi.new(api_client)
        end
    end

    def self.sources_api_client(tenant = nil)
      @sources_api_client ||= {}
      @sources_api_client[tenant] ||=
        begin
          api_client = SourcesApiClient::ApiClient.new
          api_client.default_headers.merge!(identity_headers(tenant)) if tenant
          SourcesApiClient::DefaultApi.new(api_client)
        end
    end

    module ApiClient
      def self.included(base)
        base.extend(ClassMethods)
      end

      module ClassMethods
        def sources_api_client(tenant = nil)
          TopologicalInventory::Sync.sources_api_client(tenant)
        end
      end

      def identity_headers(tenant)
        TopologicalInventory::Sync.identity_headers(tenant)
      end

      def sources_api_client(tenant = nil)
        TopologicalInventory::Sync.sources_api_client(tenant)
      end

      def topological_api_client(tenant = nil)
        TopologicalInventory::Sync.topological_api_client(tenant)
      end
    end
  end
end
