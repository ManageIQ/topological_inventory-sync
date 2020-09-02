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

        loop do
          collection = resource_id.nil? ? block.call(limit, offset) : block.call(resource_id, limit, offset)
          %i[data meta].each do |method|
            raise "Provided block expects Sources API Client response" unless collection.respond_to?(method)
          end
          break if collection.data.blank?

          collection.data.each { |record| yield record }
          offset += limit
          break if offset >= collection.meta.count.to_i
        end
      end
    end
  end
end
