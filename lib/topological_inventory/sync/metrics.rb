require "prometheus_exporter"
require "prometheus_exporter/server"
require "prometheus_exporter/client"
require "prometheus_exporter/instrumentation"

module TopologicalInventory
  class Sync
    class Metrics
      def initialize(port = 9394)
        return if port == 0

        configure_server(port)
        configure_metrics
      end

      def record_error
        @errors_counter&.observe(1)
      end

      private

      def configure_server(port)
        @server = PrometheusExporter::Server::WebServer.new(:port => port)
        @server.start

        PrometheusExporter::Client.default = PrometheusExporter::LocalClient.new(:collector => @server.collector)
      end

      def configure_metrics
        PrometheusExporter::Instrumentation::Process.start

        PrometheusExporter::Metric::Base.default_prefix = default_prefix

        @errors_counter = PrometheusExporter::Metric::Counter.new("errors_total", "total number of collector errors")
        @server.collector.register_metric(@errors_counter)
      end

      def default_prefix
        raise NotImplementedError, "#{__method__} must be implemented in a subclass"
      end
    end
  end
end
