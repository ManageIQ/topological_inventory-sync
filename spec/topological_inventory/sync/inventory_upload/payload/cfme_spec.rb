require "topological_inventory/sync/inventory_upload/payload"
require_relative "../../../../helpers/inventory_upload_helper"

RSpec.describe TopologicalInventory::Sync::InventoryUpload::Payload::Cfme do
  include InventoryUploadHelper

  context "#process" do
    let(:source) { SourcesApiClient::Source.new }
    let(:application) { SourcesApiClient::Application.new }
    it "parses cfme inventory and sends to ingress-api" do
      payload = described_class.new(cfme_inventory, "12345")

      expect(payload).to receive(:find_or_create_source).and_return(source)
      expect(payload).to receive(:find_or_create_application).and_return(application)
      expect(payload).to receive(:send_to_ingress_api)

      payload.process
    end
  end
end
