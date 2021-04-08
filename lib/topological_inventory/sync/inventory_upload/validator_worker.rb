require "json"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/inventory_upload/payload"
require "topological_inventory/sync/clowder_config"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ValidatorWorker < Worker
        include Logging

        def worker_name
          "Topological Inventory Insights Upload Validator Worker"
        end

        def queue_name
          TopologicalInventory::Sync::ClowderConfig.kafka_topic("platform.upload.topological-inventory")
        end

        def persist_ref
          "topological-inventory-upload-validator"
        end

        def perform(message)
          payload = JSON.parse(message.payload)

          account, request_id = payload.values_at("account", "request_id")
          log_header = "account [#{account}] request_id [#{request_id}]"

          logger.info("#{log_header}: Validating payload [#{request_id}]...")

          valid = "failure"
          reason = nil

          begin
            Payload.unpack(payload["url"]) do |inventory|
              # Return invalid if any of the payloads are invalid
              if valid_payload?(inventory)
                valid = "success"
              else
                reason = "Invalid payload"
              end
            end
          rescue Zlib::GzipFile::Error, JSON::Stream::ParserError => err
            reason = "Invalid file - #{err}"
          end

          payload["validation"] = valid

          logger.info("#{log_header}: Validating payload [#{request_id}]...Complete - #{payload["validation"]}#{reason ? ", #{reason}" : ""}")

          publish_validation(payload)
        end

        private

        def valid_payload?(payload)
          supported_schema_types = %w[Default Cfme].freeze

          schema_type = payload.dig("schema", "name")
          return false unless supported_schema_types.include?(schema_type)

          send("valid_#{schema_type.downcase}_payload?", payload)
        end

        def valid_cfme_payload?(payload)
          # TODO: Add additional validation checks here
          true
        end

        def valid_default_payload?(_)
          # TODO: Add additional validation checks here
          true
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
