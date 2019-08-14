require "topological_inventory-ingress_api-client"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class Payload
        class Default < Payload
          def process
            inventory = TopologicalInventoryIngressApiClient::Inventory.new
            inventory.build_from_hash(payload.deep_symbolize_keys)

            find_or_create_source(inventory.source_type, inventory.name, inventory.source)
            send_to_ingress_api(inventory)
          end
        end
      end
    end
  end
end
