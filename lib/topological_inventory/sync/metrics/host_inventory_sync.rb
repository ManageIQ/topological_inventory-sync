require "topological_inventory/sync/metrics"

module TopologicalInventory
  class Sync
    class Metrics
      class HostInventorySync < TopologicalInventory::Sync::Metrics
        ERROR_TYPES = %i[bad_request request response response_timeout].freeze

        def default_prefix
          "topological_inventory_host_inventory_sync_"
        end
      end
    end
  end
end
