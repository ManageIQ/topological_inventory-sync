require_relative '../../../helpers/inventory_upload_helper'
require "sources-api-client"
require "topological_inventory/sync"
require "topological_inventory/sync/inventory_upload/processor_worker"

RSpec.describe TopologicalInventory::Sync::InventoryUpload::ProcessorWorker do
  include InventoryUploadHelper

  context "#perform" do
    let(:processor) { described_class.new("localhost", "9092") }
    let(:message) { ManageIQ::Messaging::ReceivedMessage.new(nil, nil, insights_upload_payload, nil, nil, nil) }

    it "calls Payload process" do
      expect(TopologicalInventory::Sync::InventoryUpload::Payload).to receive(:load).with(JSON.parse(message.payload))
      processor.perform(message)
    end
  end
end
