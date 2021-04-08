require "json"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/inventory_upload/payload"
require "topological_inventory-ingress_api-client"
require "topological_inventory/providers/common/save_inventory/saver"
require "topological_inventory/sync/clowder_config"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ProcessorWorker < Worker
        include Logging

        def worker_name
          "Topological Inventory Insights Upload Processor Worker"
        end

        def queue_name
          TopologicalInventory::Sync::ClowderConfig.kafka_topic("platform.upload.available")
        end

        def persist_ref
          "topological-inventory-upload-processor"
        end

        def perform(message)
          data = JSON.parse(message.payload)
          return unless data["service"] == "topological-inventory"

          log_header = "account [#{data["account"]}] request_id [#{data["request_id"]}]"
          logger.info("#{log_header}: Processing payload [#{data["request_id"]}]...")

          Payload.load(data, &:process)

          logger.info("#{log_header}: Processing payload [#{data["request_id"]}]...Complete")
        end
      end
    end
  end
end
