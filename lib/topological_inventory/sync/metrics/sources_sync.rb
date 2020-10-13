require "topological_inventory/sync/metrics"

module TopologicalInventory
  class Sync
    class Metrics
      class SourcesSync < TopologicalInventory::Sync::Metrics
        private

        def source_created(cnt = 1)
          @sources_created_counter&.observe(cnt)
        end

        def source_destroyed(cnt = 1)
          @sources_destroyed_counter&.observe(cnt)
        end

        def configure_metrics
          super

          @sources_created_counter = PrometheusExporter::Metric::Counter.new("sources_created", "total number of created sources")
          @sources_destroyed_counter = PrometheusExporter::Metric::Counter.new("sources_destroyed", "total number of destroyed sources")

          @server.collector.register_metric(@sources_created_counter)
          @server.collector.register_metric(@sources_destroyed_counter)
        end

        def default_prefix
          "topological_inventory_sources_sync_"
        end
      end
    end
  end
end
