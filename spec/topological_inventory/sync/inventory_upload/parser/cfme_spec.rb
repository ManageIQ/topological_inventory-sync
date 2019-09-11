require "topological_inventory/sync/inventory_upload/parser"
require 'topological_inventory-ingress_api-client'

RSpec.describe TopologicalInventory::Sync::InventoryUpload::Parser::Cfme do
  it "parses missing Host#memory as zero" do
    expect(TopologicalInventoryIngressApiClient::Host).to receive(:new).with(hash_including(:memory => 0))
    described_class.new('XXX', 'XXX', 'XXX', {}).send(:parse_host, {})
  end
end
