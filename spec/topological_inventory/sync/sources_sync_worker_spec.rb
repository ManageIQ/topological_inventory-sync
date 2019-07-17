require "topological_inventory/sync"

RSpec.describe TopologicalInventory::Sync::SourcesSyncWorker do
  context "#initial_sync" do
  end

  context "#perform" do
    let(:sources_sync) do
      described_class.new("localhost", "9092")
    end
    let(:message)         { ManageIQ::Messaging::ReceivedMessage.new(nil, event, payload, headers, nil, nil) }
    let(:external_tenant) { SecureRandom.uuid }
    let(:x_rh_identity)   { Base64.strict_encode64(JSON.dump({"identity" => {"account_number" => external_tenant}})) }
    let(:payload) do
      {"name" => "AWS", "source_type_id" => "1", "uid" => SecureRandom.uuid, "id" => "1"}
    end
    let(:headers) do
      {"x-rh-identity" => x_rh_identity, "encoding" => "json"}
    end

    context "source create event" do
      let(:event) { "Source.create" }
      context "with no existing tenants" do
        it "creates a source and a new tenant" do
          sources_sync.send(:perform, message)

          expect(Source.count).to eq(1)

          source = Source.first
          expect(source.uid).to eq(payload["uid"])
          expect(source.id).to  eq(payload["id"].to_i)

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
          expect(source.uid).to eq(payload["uid"])
          expect(source.id).to  eq(payload["id"].to_i)

          expect(Tenant.count).to eq(1)
          expect(Tenant.first.external_tenant).to eq(external_tenant)
        end
      end
    end

    context "source destroy event" do
      let(:event) { "Source.destroy" }
      let(:tenant) { Tenant.find_or_create_by(:external_tenant => external_tenant) }
      let!(:source) { Source.create!(:tenant => tenant, :uid => payload["uid"]) }
      let(:payload) do
        {"name" => "AWS", "source_type_id" => "1", "tenant" => SecureRandom.uuid, "uid" => SecureRandom.uuid, "id" => "1"}
      end

      it "deletes the source" do
        sources_sync.send(:perform, message)
        expect(Source.count).to eq(0)
      end
    end
  end
end
