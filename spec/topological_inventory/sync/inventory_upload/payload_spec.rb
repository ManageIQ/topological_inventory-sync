require "topological_inventory/sync/inventory_upload/payload"
require_relative "../../../helpers/inventory_upload_helper"

RSpec.describe TopologicalInventory::Sync::InventoryUpload::Payload do
  include InventoryUploadHelper

  let(:inventory) { {"schema" => {"name" => "Cfme"}} }
  let(:url) { "http://localhost/payload.tar.gz" }

  context ".load" do
    it "returns a properly constructed payload object" do
      expect(described_class).to receive(:open_url).with(url).and_yield(targz(inventory))

      payload = nil
      described_class.load("account" => "12345", "url" => url) { |p| payload = p }
      expect(payload.class.name).to eq("TopologicalInventory::Sync::InventoryUpload::Payload::Cfme")
    end
  end

  context ".unpack" do
    it "yields the payload" do
      expect(described_class).to receive(:open_url).with(url).and_yield(targz(inventory))

      result = nil
      described_class.unpack(url) { |json| result = json }
      expect(result).to eq(inventory)
    end
  end

  context "#find_or_create_source (private)" do
    let(:source_type) { SourcesApiClient::SourceType.new(:id => "1", :name => "openshift") }
    let(:source_types) { [source_type] }
    let(:source) { SourcesApiClient::Source.new(:name => "mysource", :uid => SecureRandom.uuid) }
    let(:sources) { [source] }
    let(:sources_api_client) { double }
    let(:payload) { described_class.new(ocp_inventory, "12345") }

    before do
      source_type_collection = double
      allow(source_type_collection).to receive(:data).and_return(source_types)

      sources_collection = double
      allow(sources_collection).to receive(:data).and_return(sources)

      allow(sources_api_client).to receive(:list_source_types).and_return(source_type_collection)
      allow(sources_api_client).to receive(:list_sources).and_return(sources_collection)
      allow(payload).to receive(:sources_api_client).and_return(sources_api_client)
    end

    context "with an invalid source type" do
      let(:source_types) { [] }

      it "raises exception" do
        expect { payload.send(:find_or_create_source, "abcd", "name", "uid") }
          .to raise_exception(RuntimeError, "Failed to find source type [abcd]")
      end
    end

    context "with no existing sources" do
      let(:sources) { [] }

      it "creates the new source" do
        expect(sources_api_client).to receive(:create_source_with_http_info).and_return([source, 201])
        expect(payload.send(:find_or_create_source, source_type.name, source.name, source.uid)).to eq(source)
      end

      it "raises an error if sources-api-client fails" do
        expect(sources_api_client).to receive(:create_source_with_http_info)
          .and_raise(SourcesApiClient::ApiError.new(:code => 400, :response_headers => {}, :response_body => "Bad Request"))
        expect { payload.send(:find_or_create_source, source_type.name, source.name, source.uid) }
          .to raise_exception("Failed to create source [#{source.name}] [#{source.uid}] [openshift]: Bad Request")
      end
    end

    context "with an existing source" do
      it "returns the existing source" do
        expect(sources_api_client).not_to receive(:create_source_with_http_info)
        expect(payload.send(:find_or_create_source, source_type.name, source.name, source.uid)).to eq(source)
      end
    end
  end
end
