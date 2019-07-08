require_relative '../../../helpers/inventory_upload_helper'
require "sources-api-client"
require "topological_inventory/sync"
require "topological_inventory/sync/inventory_upload/processor_worker"

RSpec.describe TopologicalInventory::Sync::InventoryUpload::ProcessorWorker do
  include InventoryUploadHelper

  context "#perform" do
    let(:processor) do
      described_class.new("localhost", "9092")
    end
    let(:message) { ManageIQ::Messaging::ReceivedMessage.new(nil, nil, payload, nil, nil, nil) }
    let(:request_id) { "52df9f748eabcfeb" }
    let(:file_path) { "/tmp/uploads/insights-upload-perm-test/#{request_id}" }
    let(:payload) { insights_upload_payload }
    let(:inventory) { ocp_inventory }

    context "with missing Source Type" do
      before do
        expect(TopologicalInventory::Sync::InventoryUpload::Parser)
          .to receive(:open_url).and_yield(targz(inventory))

        source_type_collection = double
        allow(source_type_collection).to receive(:data).and_return(source_type)

        allow_any_instance_of(SourcesApiClient::DefaultApi).to receive(:list_source_types).and_return(source_type_collection)
      end

      let(:source_type) { [] }

      it "raises exception when Source Type not found" do
        expect { processor.send(:perform, message) }.to raise_exception("Source Type #{inventory['source_type']} not found!")
      end
    end

    context "calling find_or_create_source" do
      let(:sources_api) { double }
      let(:source_data) { {:name => inventory['name'], :uid => inventory['source']} }
      let(:source) { double }


      it "returns existing Source" do
        allow(sources_api).to receive(:list_sources).and_return(source)
        allow(source).to receive(:data).and_return([source_data])

        response = processor.send(:find_or_create_source, sources_api, 1, inventory['name'], inventory['source'])

        expect(response).to eq(source_data)
      end

      context "when Source is not found" do
        it "creates new Source" do
          allow(sources_api).to receive(:list_sources).and_return(source)
          allow(source).to receive(:data).and_return([])

          allow(sources_api).to receive(:create_source_with_http_info).and_return([source_data, 201, nil])

          response = processor.send(:find_or_create_source, sources_api, '1', inventory['name'], inventory['source'])

          expect(response).to eq(source_data)
        end

        it "raises error when sources-api doesn't response with HTTP 201" do
          allow(sources_api).to receive(:list_sources).and_return(source)
          allow(source).to receive(:data).and_return([])

          allow(sources_api).to receive(:create_source_with_http_info).and_return([source_data, 400, nil])

          expect { processor.send(:find_or_create_source,
                                  sources_api,
                                  '1',
                                  inventory['name'],
                                  inventory['source'])}.to raise_exception("Failed to create Source #{ocp_inventory['name']} (#{ocp_inventory['source']})")
        end
      end

      context "sending to Ingress API" do
        let(:ingress_api_sender) { double }
        let(:total_parts) { 5 }
        let(:source_type) { double }
        let(:inventory) { ocp_inventory.merge('refresh_state_uuid' => '1',
                                              'collections' => [{ 'name' => 'some_collection' }])}

        before do
          expect(TopologicalInventory::Sync::InventoryUpload::Parser)
            .to receive(:open_url).and_yield(targz(inventory))

          allow(ingress_api_sender).to receive(:save).and_return(total_parts)
          allow(processor).to receive(:sources_api_client).and_return(double)
          allow(processor).to receive(:find_source_type).and_return(source_type)
          allow(source_type).to receive(:id).and_return(double)
          allow(processor).to receive(:find_or_create_source).and_return(double)
          allow(processor).to receive(:ingress_api_sender).and_return(ingress_api_sender)
        end

        it "sends inventory and mark&sweep metadata" do
          expect(ingress_api_sender).to receive(:save).twice

          processor.send(:perform, message)
        end

        it "sends received inventory" do
          expect(ingress_api_sender).to receive(:save).with(hash_including(:inventory => inventory))

          processor.send(:perform, message)
        end

        it "sends total parts inventory" do
          expect(TopologicalInventoryIngressApiClient::Inventory).to receive(:new).with(hash_including(:total_parts => total_parts, :sweep_scope => %w[some_collection]))

          processor.send(:perform, message)
        end
      end
    end
  end
end
