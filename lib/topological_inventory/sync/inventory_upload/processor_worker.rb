require "json"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/inventory_upload/parser"
require "topological_inventory-ingress_api-client"
require "topological_inventory-ingress_api-client/save_inventory/saver"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ProcessorWorker < Worker
        include Logging

        def worker_name
          "Topological Inventory Insights Upload Processor Worker"
        end

        def queue_name
          "platform.upload.available"
        end

        def persist_ref
          "topological-inventory-upload-processor"
        end

        def perform(message)
          payload = JSON.parse(message.payload)
          return unless payload["service"] == "topological-inventory"

          account, request_id, payload_id = payload.values_at("account", "request_id", "payload_id")
          log_header = "account [#{account}] request_id [#{request_id}]"

          logger.info("#{log_header}: Processing payload [#{payload_id}]...")

          inventory = TopologicalInventory::Sync::InventoryUpload::Parser.parse_inventory_payload(payload['url'])

          process_inventory(inventory, account)
        end

        private

        # @param inventory [Hash]
        # @param account [String] account from x-rh-identity header
        def process_inventory(inventory, account)
          sources_api = sources_api_client(account)

          source_type = find_source_type(inventory['source_type'], sources_api)

          find_or_create_source(sources_api, source_type.id, inventory['name'], inventory['source'])

          new_inventory = convert_to_topological_inventory_schema(inventory)
          send_to_ingress_api(new_inventory)
        end

        def find_source_type(source_type_name, sources_api)
          raise 'Missing Source Type name!' if source_type_name.blank?

          response = sources_api.list_source_types({:filter => {:name => source_type_name}})
          if response.data.blank?
            raise "Source Type #{source_type_name} not found!"
          else
            logger.info("Source Type #{source_type_name} found")
            response.data.first
          end
        end

        def find_or_create_source(sources_api, source_type_id, source_name, source_uid)
          return if source_name.nil?

          sources = sources_api.list_sources({:filter => {:uid => source_uid}})

          if sources.data.blank?
            source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => source_type_id)
            source, status_code, _ = sources_api.create_source_with_http_info(source)

            if status_code == 201
              logger.info("Source #{source_name}(#{source_uid}) created successfully")
              source
            else
              raise "Failed to create Source #{source_name} (#{source_uid})"
            end
          else
            logger.debug("Source #{source_name} (#{source_uid}) found")
            sources.data.first
          end
        end

        # TODO: Now it handles only "Default" schema
        def convert_to_topological_inventory_schema(inventory)
          inventory
        end

        def send_to_ingress_api(inventory)
          logger.info("[START] Send to Ingress API with :refresh_state_uuid => '#{inventory['refresh_state_uuid']}'...")

          sender = ingress_api_sender

          # Send data to ingress_api
          total_parts = sender.save(:inventory => inventory)

          # Send total parts sent to ingress_api
          sender.save(
            :inventory => inventory_for_sweep(inventory, total_parts)
          )

          logger.info("[COMPLETED] Send to Ingress API with :refresh_state_uuid => '#{inventory['refresh_state_uuid']}'. Total parts: #{total_parts}")
          total_parts
        end

        def inventory_for_sweep(inventory, total_parts)
          TopologicalInventoryIngressApiClient::Inventory.new(
            :name => inventory['name'],
            :schema => TopologicalInventoryIngressApiClient::Schema.new(:name => inventory['schema']['name']),
            :source => inventory['source'],
            :collections => [],
            :refresh_state_uuid => inventory['refresh_state_uuid'],
            :total_parts => total_parts,
            :sweep_scope => inventory['collections'].collect { |collection| collection['name'] }.compact
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
      end
    end
  end
end
