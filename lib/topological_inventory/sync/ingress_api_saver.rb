require "topological_inventory-ingress_api-client/save_inventory/saver"

module TopologicalInventory
  class Sync
    class IngressApiSaver < TopologicalInventoryIngressApiClient::SaveInventory::Saver
      def save(data)
        inventory      = data[:inventory].to_hash
        inventory_json = data[:inventory_json] || JSON.generate(inventory)

        if inventory_json.size < max_bytes
          save_inventory(inventory_json)
          return 1
        else
          # GC can clean this up
          inventory_json = nil
          return save_payload_in_batches(inventory)
        end
      end
    end
  end
end
