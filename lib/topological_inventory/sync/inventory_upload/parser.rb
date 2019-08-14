require "topological_inventory/sync/inventory_upload/parser/cfme"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class Parser
        class << self
          def parse(source_type, source_uid, source_name, payload)
            new(source_type, source_uid, source_name, payload).parse
          end
        end

        def initialize(source_type, source_uid, source_name, payload)
          @source_type = source_type
          @source_uid  = source_uid
          @source_name = source_name
          @payload     = payload
        end

        def parse
          raise NotImplementedError
        end

        private

        attr_reader :source_type, :source_uid, :source_name, :payload

        def ingress_api_inventory
          TopologicalInventoryIngressApiClient::Inventory.new(
            :source                  => source_uid,
            :source_type             => source_type,
            :schema                  => TopologicalInventoryIngressApiClient::Schema.new(:name => "Default"),
            :name                    => source_name,
            :refresh_state_uuid      => SecureRandom.uuid,
            :refresh_state_part_uuid => SecureRandom.uuid,
            :collections             => [],
          )
        end

        def ingress_api_inventory_collection(name, data = [])
          TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => name, :data => data)
        end

        def ingress_api_lazy_ref(collection_name, reference, ref = :manager_ref)
          reference = {:source_ref => reference} unless reference.kind_of?(Hash)

          TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
            :inventory_collection_name => collection_name, :reference => reference, :ref => ref
          )
        end
      end
    end
  end
end
