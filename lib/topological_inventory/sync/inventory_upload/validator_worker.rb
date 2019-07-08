require "json"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/inventory_upload/parser"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ValidatorWorker < Worker
        include Logging

        def worker_name
          "Topological Inventory Insights Upload Validator Worker"
        end

        def queue_name
          "platform.upload.topological-inventory"
        end

        def persist_ref
          "topological-inventory-upload-validator"
        end

        def perform(message)
          payload = JSON.parse(message.payload)

          account, request_id, payload_id = payload.values_at("account", "request_id", "payload_id")
          log_header = "account [#{account}] request_id [#{request_id}]"

          logger.info("#{log_header}: Validating payload [#{payload_id}]...")

          inventory = Parser.parse_inventory_payload(payload["url"])
          payload["validation"] = valid_payload?(inventory) ? "success" : "failure"

          logger.info("#{log_header}: Validating payload [#{payload_id}]...Complete - #{payload["validation"]}")

          publish_validation(payload)
        end

        private

        def valid_payload?(payload)
          require "topological_inventory/schema"

          schema_klass = schema_klass_name(payload.dig("schema", "name")).safe_constantize
          schema_klass.present?
        end

        def schema_klass_name(name)
          "TopologicalInventory::Schema::#{name}"
        end

        def publish_validation(payload)
          messaging_client.publish_topic(
            :service => "platform.upload.validation",
            :event   => "", # TODO we shouldn't require this in MIQ-Messaging
            :payload => payload.to_json
          )
        end
      end
    end
  end
end
