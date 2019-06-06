require "json"
require "topological_inventory/sync/worker"
require 'typhoeus'
require "topological_inventory-ingress_api-client"
require "topological_inventory/sync/ingress_api_saver"

module TopologicalInventory
  class Sync
    class InventoryUploadWorker < Worker
      include Logging

      def queue_name
        "platform.upload.available"
      end

      def persist_ref
        "topological-inventory-upload-processor"
      end

      def worker_name
        "Topological Inventory Inventory Upload Worker"
      end

      def perform(message)
        payload = JSON.parse(message.payload)
        return unless payload["service"] == "topological-inventory"

        openurl(payload["url"]) do |io|
          untargz(io) do |file|
            process_data(payload, file.read)
          end
        end
      end

      private

      def process_data(payload, file)
        sources_api = sources_api_client(payload["rh_account"])
        data = JSON.parse(file)

        # Find Source Type
        source_type = find_source_type(data["source_type"], sources_api)
        # Create source with first payload
        source = find_or_create_source(sources_api, source_type.id, data['name'], data["source"])

        if source.present?
          send_to_ingress_api(data, file)
        else
          raise "Failed to create Source"
        end
      end

      def openurl(url)
        require "http"

        uri = URI(url)
        if uri.scheme.nil?
          File.open(url) { |f| yield f }
        elsif uri.scheme == 'file'
          request = Typhoeus::Request.new(url)
          response = request.run

          if response.return_code == :ok # response.success? doesn't work for file:///...
            yield StringIO.new(response.body)
          else
             failed_to_get_file_error(response)
          end
        else
          response = HTTP.get(uri)
          response.body.stream!
          yield response.body
        end
      end

      def untargz(io)
        require "rubygems/package"
        Zlib::GzipReader.wrap(io) do |gz|
          Gem::Package::TarReader.new(gz) do |tar|
            tar.each { |entry| yield entry }
          end
        end
      end

      def download_file(url)
        request = Typhoeus::Request.new(url)
        request.run
      end

      def find_source_type(source_type_name, sources_api)
        return if source_type_name.nil?

        response = sources_api.list_source_types({:filter => {:name => source_type_name}})
        if response.data.blank?
          raise "Source Type #{source_type_name} not found!"
        else
          response.data.first
        end
      end

      def find_or_create_source(sources_api, source_type_id, source_name, source_uid)
        return if source_name.nil?

        sources = sources_api.list_sources({:filter => {:uid => source_uid}})

        if sources.data.blank?
          source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => source_type_id)
          sources_api.create_source(source)
        else
          sources.data.first
        end
      rescue Exception => e
        logger.error("Failed to get or create Source: #{source_uid} - #{e.message}")
        raise e
      end

      def send_to_ingress_api(data, data_json)
        refresh_state_uuid = data['refresh_state_uuid']
        logger.info("Send to Ingress API with :refresh_state_uuid => '#{refresh_state_uuid}'...")

        total_parts = 0

        total_parts += ingress_api_saver.save(
          :inventory => data,
          :inventory_json => data_json
        )

        # TODO: It expects we receive all data in one file
        # TODO: If sender will work as collector (save&sweep), then total_parts has to be added from sender
        # Sweep
        inventory = TopologicalInventoryIngressApiClient::Inventory.new(
          :name => data['name'],
          :schema => TopologicalInventoryIngressApiClient::Schema.new(:name => data['schema']['name']),
          :source => data['source'],
          :collections => [],
          :refresh_state_uuid => refresh_state_uuid,
          :total_parts => total_parts,
          :sweep_scope => data['collections'].collect { |collection| collection['name'] }.compact
        )

        ingress_api_saver.save(
           :inventory => inventory
        )

      end

      def ingress_api_client
        TopologicalInventoryIngressApiClient::DefaultApi.new
      end

      def ingress_api_saver
        TopologicalInventory::Sync::IngressApiSaver.new(
          :client => ingress_api_client,
          :logger => logger
        )
      end

      def failed_to_get_file_error(response)
        msg = ["Failed to get uploaded file: "]
        {
          :code => response.code,
          :response_headers => response.headers,
          :response_body => response.body,
          :response_status_message => response.status_message
        }.each_pair do |header, value|
          msg << "#{header}: #{value}" unless value.blank?
        end
        raise msg.join(", ")
      end
    end
  end
end
