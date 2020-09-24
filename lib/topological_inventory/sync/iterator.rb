module TopologicalInventory
  class Sync
    class Iterator
      attr_reader :block, :limit, :resource_id

      def initialize(block, limit: 100, resource_id: nil)
        @block = block
        @limit = limit
        @resource_id = resource_id
      end

      def each
        offset = 0
        finished = false

        until finished
          collection = resource_id.nil? ? block.call(limit, offset) : block.call(resource_id, limit, offset)
          %i[data meta].each { |method| raise "Sources API Client response expected" unless collection.respond_to?(method) }
          break if collection.data.blank?

          collection.data.each { |record| yield record }
          offset += limit

          finished = true if offset >= collection.meta.count.to_i
        end
      end
    end
  end
end
