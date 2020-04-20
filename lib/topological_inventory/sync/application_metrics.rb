require "benchmark"
require "prometheus_exporter"
require "prometheus_exporter/server"
require "prometheus_exporter/client"
require 'prometheus_exporter/instrumentation'

module TopologicalInventory
  class Sync
    class ApplicationMetrics
      def initialize(port, default_prefix)
        return if port == 0

        @default_prefix = default_prefix

        configure_server(port)
        configure_metrics
      end

      def stop_server
        @server&.stop
      end

      private

      def configure_server(port)
        @server = PrometheusExporter::Server::WebServer.new(:port => port)
        @server.start

        PrometheusExporter::Client.default = PrometheusExporter::LocalClient.new(:collector => @server.collector)
      end

      def configure_metrics
        PrometheusExporter::Instrumentation::Process.start
        PrometheusExporter::Metric::Base.default_prefix = @default_prefix
      end
    end
  end
end
