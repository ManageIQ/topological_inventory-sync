require "json"
require "topological_inventory/sync/worker"
require "pry-byebug"
require 'typhoeus'

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

      def perform(message)
        payload = JSON.parse(message.payload)
        return unless payload["service"] == "topological-inventory"

        request = Typhoeus::Request.new(payload["url"])
        response = request.run

        if response.return_code == :ok # response.success? doesn't work for file:///...
          data = JSON.parse(response.body)

          source = find_or_create_source(data["name"], data["source"], payload["rh_account"]) unless data["source"].nil?

          if source.present?
            send_to_ingress_api(source, data, payload["rh_account"])
          else
            # TODO: Ack message in kafka when error occurs?
          end
        else
          msg = ["Failed to get uploaded file: "]
          {
            :code => response.code,
            :response_headers => response.headers,
            :response_body => response.body,
            :response_status_message => response.status_message
          }.each_pair do |header, value|
            msg << "#{header}: #{value}" unless value.blank?
          end
          logger.error(msg.join(", "))
        end
      end

      private

      def send_to_ingress_api(source, data, external_tenant)

      end

      def find_or_create_source(source_name, source_uid, external_tenant)
        api_client = sources_api_client(external_tenant)

        sources = api_client.list_sources({:filter => {:uid => source_uid}})

        if sources.data.blank?
          # TODO: find_or_create_source_type()

          source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => '1')
          api_client.create_source(source)
        else
          sources.data.first
        end
      rescue Exception => e
        logger.error("Failed to get or create Source: #{source_uid} - #{e.message}")
        raise e
      end
    end
  end
end
