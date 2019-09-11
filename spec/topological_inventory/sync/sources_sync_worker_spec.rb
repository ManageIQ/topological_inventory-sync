require "topological_inventory/sync"
require "sources-api-client"

RSpec.describe TopologicalInventory::Sync::SourcesSyncWorker do
  let(:sources_sync)       { described_class.new("localhost", "9092") }
  let(:source)             { {"id" => "1", "uid" => SecureRandom.uuid} }
  let(:sources_api_client) { double("SourcesApiClient::Default") }

  before do
    allow(sources_api_client).to receive(:list_application_types).and_return(
      SourcesApiClient::ApplicationTypesCollection.new(
        :data  => [
          SourcesApiClient::ApplicationType.new(
            :name                   => "/insights/platform/catalog",
            :dependent_applications => ["/insights/platform/topological-inventory"],
            :id                     => "1"
          ),
          SourcesApiClient::ApplicationType.new(
            :name                   => "/insights/platform/cost-management",
            :dependent_applications => [],
            :id                     => "2"
          ),
          SourcesApiClient::ApplicationType.new(
            :name                   => "/insights/platform/topological-inventory",
            :dependent_applications => [],
            :id                     => "3"
          )
        ],
        :links => {}
      )
    )
    allow(sources_api_client).to receive(:show_source).and_return(SourcesApiClient::Source.new(source))
    allow(described_class).to    receive(:sources_api_client).and_return(sources_api_client)
  end

  context "#initial_sync" do
    before do
      identity = Base64.strict_encode64(
        JSON.dump({"identity" => {"account_number" => "topological_inventory-sources_sync"}}))
      allow(RestClient).to receive(:get)
        .with("http://cloud.redhat.com:443/internal/v1.0/tenants", {"x-rh-identity"=> identity})
        .and_return("[{\"external_tenant\":\"12345\"}]")

      allow(sources_api_client).to receive(:list_applications).and_return(
        SourcesApiClient::ApplicationsCollection.new(:data => applications, :links => {})
      )

      allow(sources_api_client).to receive(:list_sources).and_return(
        SourcesApiClient::ApplicationsCollection.new(:data => sources, :links => {})
      )
    end

    context "with a source added" do
      let(:sources)      { [SourcesApiClient::Source.new(:id => "1", :uid => SecureRandom.uuid)] }
      let(:applications) { [SourcesApiClient::Application.new(:source_id => "1", :application_type_id => "3")] }

      it "creates a new source" do
        expect { sources_sync.initial_sync }.to change { Source.count }.by(1)
      end
    end

    context "with a source deleted" do
      let(:tenant)       { Tenant.create(:external_tenant => SecureRandom.uuid) }
      let(:source)       { Source.create!(:uid => SecureRandom.uuid, :tenant => tenant) }
      let(:sources)      { [] }
      let(:applications) { [] }

      it "deletes the source" do
        expect { sources_sync.initial_sync }.to change { Source.count }.by(-1)
      end
    end
  end

  context "#perform" do
    let(:message)         { ManageIQ::Messaging::ReceivedMessage.new(nil, event, payload, headers, nil, nil) }
    let(:external_tenant) { SecureRandom.uuid }
    let(:x_rh_identity)   { Base64.strict_encode64(JSON.dump({"identity" => {"account_number" => external_tenant}})) }
    let(:headers)         { {"x-rh-identity" => x_rh_identity, "encoding" => "json"} }

    context "Application create event" do
      let(:event) { "Application.create" }

      context "with a source not enabled for topology" do
        let(:payload) do
          {"source_id" => "1", "application_type_id" => "2"}
        end

        it "doesn't create the source" do
          sources_sync.send(:perform, message)
          expect(Source.find_by(:uid => source["uid"])).to be_nil
        end
      end

      context "with a source enabled for topology" do
        let(:payload) do
          {"source_id" => "1", "application_type_id" => "1"}
        end

        context "with no existing tenants" do
          it "creates a source and a new tenant" do
            sources_sync.send(:perform, message)

            expect(Source.count).to eq(1)

            source = Source.first
            expect(source.uid).to eq(source["uid"])
            expect(source.id).to  eq(source["id"].to_i)

            expect(Tenant.count).to eq(1)
            expect(Tenant.first.external_tenant).to eq(external_tenant)
          end
        end

        context "with an existing tenant" do
          let(:tenant) { Tenant.find_or_create_by(:external_tenant => external_tenant) }

          it "creates a source on an existing tenant" do
            sources_sync.send(:perform, message)

            expect(Source.count).to eq(1)

            source = Source.first
            expect(source.uid).to eq(source["uid"])
            expect(source.id).to  eq(source["id"].to_i)

            expect(Tenant.count).to eq(1)
            expect(Tenant.first.external_tenant).to eq(external_tenant)
          end
        end
      end
    end

    context "source destroy event" do
      let(:tenant) { Tenant.find_or_create_by(:external_tenant => external_tenant) }
      let!(:source) { Source.create!(:tenant => tenant, :uid => SecureRandom.uuid) }

      context "when the source was deleted" do
        let(:event) { "Source.destroy" }
        let(:payload) do
          {"name" => "AWS", "source_type_id" => "1", "tenant" => tenant.external_tenant, "uid" => source.uid, "id" => source.id}
        end

        it "deletes the source" do
          expect { sources_sync.send(:perform, message) }.to change { Source.count }.by(-1)
        end
      end

      context "when the source was disabled for topology" do
        let(:event) { "Application.destroy" }
        let(:payload) do
          {"source_id" => source.id, "tenant" => tenant.external_tenant, "application_type_id": "1"}
        end

        it "deletes the source" do
          expect { sources_sync.send(:perform, message) }.to change { Source.count }.by(-1)
        end
      end
    end
  end
end
