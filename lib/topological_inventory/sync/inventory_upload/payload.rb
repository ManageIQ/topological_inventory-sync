require "sources-api-client"
require "topological_inventory/sync/inventory_upload/payload/cfme"
require "topological_inventory/sync/inventory_upload/payload/default"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class Payload
        include Logging

        class << self
          def load(message)
            account, url = message.values_at("account", "url")
            unpack(url) do |json|
              payload = payload_klass(json)&.new(json, account)
              yield payload if payload.present?
            end
          end

          def unpack(url)
            open_url(url) do |io|
              untargz(io) do |file|
                require "json/stream"
                yield JSON::Stream::Parser.parse(file)
              end
            end
          end

          private

          def payload_klass(json)
            "#{name}::#{json.dig("schema", "name")}".safe_constantize
          end

          def untargz(io)
            require "rubygems/package"
            Zlib::GzipReader.wrap(io) do |gz|
              Gem::Package::TarReader.new(gz) do |tar|
                tar.each { |entry| yield entry }
              end
            end
          end

          def open_url(url)
            require "http"

            uri = URI(url)
            if ["file", nil].include?(uri.scheme)
              File.open(uri.path) { |f| yield f }
            else
              response = HTTP.get(uri)
              response.body.stream!
              yield response.body
            end
          end
        end

        def initialize(payload, account)
          @account = account
          @payload = payload
        end

        private

        attr_reader :account, :payload

        def find_or_create_source(source_type_name, source_name, source_uid)
          sources_api = sources_api_client(account)
          source_type = find_source_type(sources_api, source_type_name)
          raise "Failed to find source type [#{source_type_name}]" if source_type.nil?

          find_source(sources_api, source_uid) || create_source(sources_api, source_uid, source_name, source_type)
        end

        def find_or_create_application(source_type_name, source)
          sources_api = sources_api_client(account)

          application_type_name = TopologicalInventory::Sync::Worker::TOPOLOGY_APP_NAME
          application_type = find_application_type(application_type_name)
          raise "Failed to find application type [#{application_type_name}]" if application_type.nil?

          find_application(sources_api, source, source_type_name, application_type.id) || create_application(sources_api, source, source_type_name, application_type.id)
        end

        def find_source_type(sources_api, source_type_name)
          sources_api.list_source_types({:filter => {:name => source_type_name}})&.data&.first
        rescue SourcesApiClient::ApiError => e
          raise "Failed to find source type [#{source_type_name}]: #{e.response_body}"
        end

        def find_application_type(application_type_name)
          external_tenant = "system_orchestrator".freeze
          sources_api_client(external_tenant).list_application_types({:filter => {:name => application_type_name}})&.data&.first
        rescue SourcesApiClient::ApiError => e
          raise "Failed to find application type [#{application_type_name}]: #{e.response_body}"
        end

        def find_application(sources_api, source, source_type_name, application_type_id)
          sources_api.list_applications({:filter => {:application_type_id => application_type_id, :source_id => source.id }})&.data&.first
        rescue SourcesApiClient::ApiError => e
          raise "Failed to find application for Source Name [#{source.name}] UID [#{source.uid}] Type [#{source_type_name}]: #{e.response_body}"
        end

        def find_source(sources_api, source_uid)
          sources_api.list_sources({:filter => {:uid => source_uid}})&.data&.first
        rescue SourcesApiClient::ApiError => e
          raise "Failed to find source [#{source_uid}]: #{e.response_body}"
        end

        def create_source(sources_api, source_uid, source_name, source_type)
          logger.info("Creating Source")
          new_source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => source_type.id)
          source, = sources_api.create_source_with_http_info(new_source)
          logger.info("Created Source: Name [#{source_name}] UID [#{source_uid}] Type [#{source_type.name}]")
          source
        rescue SourcesApiClient::ApiError => e
          raise "Failed to create source [#{source_name}] [#{source_uid}] [#{source_type.name}]: #{e.response_body}"
        end

        # Sources sync worker writes new source to topological db only if source is assigned to topological-inventory application
        def create_application(sources_api, source, source_type, application_type_id)
          return if source.nil?

          logger.info("Creating Topological-Inventory Application")

          new_app = SourcesApiClient::Application.new(:application_type_id => application_type_id,
                                                      :source_id => source.id)
          app = sources_api.create_application(new_app)
          logger.info("Created Application for Source: Name [#{source.name}] UID [#{source.uid}] Type [#{source_type}]")

          app
        rescue SourcesApiClient::ApiError => e
          raise "Failed to create application for Source: Name [#{source.name}] UID [#{source.uid}] Type [#{source_type}]: #{e.response_body}"
        end

        def send_to_ingress_api(inventory)
          logger.info("[START] Send to Ingress API with :refresh_state_uuid => '#{inventory.refresh_state_uuid}'...")

          sender = ingress_api_sender

          # Send data to ingress_api
          total_parts = sender.save(:inventory => inventory)

          # Send total parts sent to ingress_api
          sender.save(
            :inventory => inventory_for_sweep(inventory, total_parts)
          )

          logger.info("[COMPLETED] Send to Ingress API with :refresh_state_uuid => '#{inventory.refresh_state_uuid}'. Total parts: #{total_parts}")
          total_parts
        rescue => e
          response_body = e.response_body if e.respond_to? :response_body
          response_headers = e.response_headers if e.respond_to? :response_headers
          logger.error("Error when sending payload to Ingress API. Error message: #{e.message}. Body: #{response_body}. Header: #{response_headers}")
          raise
        end

        def inventory_for_sweep(inventory, total_parts)
          TopologicalInventoryIngressApiClient::Inventory.new(
            :name => inventory.name,
            :schema => TopologicalInventoryIngressApiClient::Schema.new(:name => inventory.schema.name),
            :source => inventory.source,
            :collections => [],
            :refresh_state_uuid => inventory.refresh_state_uuid,
            :total_parts => total_parts,
            :sweep_scope => inventory.collections.collect { |collection| collection.name }.compact
          )
        end

        def ingress_api_client
          TopologicalInventoryIngressApiClient::DefaultApi.new
        end

        def ingress_api_sender
          TopologicalInventoryIngressApiClient::SaveInventory::Saver.new(
            :client => ingress_api_client,
            :logger => logger
          )
        end

        def sources_api_client(tenant = nil)
          api_client = SourcesApiClient::ApiClient.new

          if tenant
            api_client.default_headers.merge!(
              {
                "x-rh-identity" => Base64.strict_encode64(
                  JSON.dump({"identity" => {"account_number" => tenant}})
                )
              }
            )
          end

          SourcesApiClient::DefaultApi.new(api_client)
        end
      end
    end
  end
end
