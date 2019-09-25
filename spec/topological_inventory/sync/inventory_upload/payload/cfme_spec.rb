require "topological_inventory/sync/inventory_upload/payload"
require_relative "../../../../helpers/inventory_upload_helper"
require 'topological_inventory-api-client'

RSpec.describe TopologicalInventory::Sync::InventoryUpload::Payload::Cfme do
  include InventoryUploadHelper

  describe '#process' do
    let(:source) { SourcesApiClient::Source.new }
    let(:application) { SourcesApiClient::Application.new }
    let(:payload) { described_class.new(cfme_inventory, '12345') }

    it 'parses cfme inventory and sends to ingress-api' do

      payload.send(:cfme_ems_types).each do |ems_type|
        cfme_inventory[ems_type].to_a.each do |provider_payload|
          expect(payload).to receive(:source_exists_in_topology_inventory?).with(provider_payload['guid']).and_return(true)
        end
      end

      expect(payload).to receive(:find_or_create_source).and_return(source)
      expect(payload).to receive(:find_or_create_application).and_return(application)
      expect(payload).to receive(:send_to_ingress_api)

      payload.process
    end

    context 'check if source exists in topology inventory' do
      let(:topological_api_client) { double("TopologicalInventoryApiClient::Default") }
      let(:source_in_topology_inventory) { source }

      before do
        payload.send(:cfme_ems_types).each do |ems_type|
          cfme_inventory[ems_type].to_a.each do |provider_payload|
            guid = provider_payload['guid']
            allow(payload).to receive(:find_source).with(topological_api_client, guid).and_return(source_in_topology_inventory)
          end
        end

        allow(TopologicalInventory::Sync).to receive(:topological_api_client).and_return(topological_api_client)
      end

      it 'parses cfme inventory and sends to ingress-api when source exists in topological inventory' do
        expect(payload).to receive(:find_or_create_source).and_return(source)
        expect(payload).to receive(:find_or_create_application).and_return(application)
        expect(payload).to receive(:send_to_ingress_api)

        payload.process
      end

      context "source doesn't exist in topological inventory yet" do
        let(:source_in_topology_inventory) { nil }

        before do
          described_class::TIMEOUT_COUNT = 0.001.seconds.freeze
        end

        it "doesn't send inventory to ingress-api" do
          expect(payload).to receive(:find_or_create_source).and_return(source)
          expect(payload).to receive(:find_or_create_application).and_return(application)
          expect(payload).not_to receive(:send_to_ingress_api)

          payload.process
        end
      end
    end
  end
end
