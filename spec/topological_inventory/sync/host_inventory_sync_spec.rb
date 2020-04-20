require "topological_inventory/sync/host_inventory_sync_worker"

RSpec.describe TopologicalInventory::Sync::HostInventorySyncWorker do
  context "#topological_inventory_api (private)" do
    it "returns the initial url if provided" do
      params_hash = described_class.send(:build_topological_inventory_url_hash, "example.com", "9092", "", "")
      host_sync_object = described_class.new(params_hash, "", "", "", 0)
      expect(host_sync_object.send(:build_topological_inventory_url_from, {})).to eq("http://example.com:9092/v3.0")
    end

    context "with service env vars set" do
      it "returns a sane value" do
        params_hash = described_class.send(:build_topological_inventory_url_hash, "example.com", "8080", "", "")
        host_sync_object = described_class.new(params_hash, "", "", "", 0)
        expect(host_sync_object.send(:build_topological_inventory_url_from, {})).to eq("http://example.com:8080/v3.0")
      end

      context "with APP_NAME set" do
        it "includes the APP_NAME" do
          params_hash = described_class.send(:build_topological_inventory_url_hash, "example.com", "8080", "", "topological-inventory")
          host_sync_object = described_class.new(params_hash, "", "", "", 0)
          expect(host_sync_object.send(:build_topological_inventory_url_from, {})).to eq("http://example.com:8080/topological-inventory/v3.0")
        end

        it "uses the PATH_PREFIX with a leading slash" do
          params_hash = described_class.send(:build_topological_inventory_url_hash, "example.com", "8080", "/this/is/a/path", "topological-inventory")
          host_sync_object = described_class.new(params_hash, "", "", "", 0)
          expect(host_sync_object.send(:build_topological_inventory_url_from, {})).to eq("http://example.com:8080/this/is/a/path/topological-inventory/v3.0")
        end

        it "uses the PATH_PREFIX without a leading slash" do
          params_hash = described_class.send(:build_topological_inventory_url_hash, "example.com", "8080", "also/a/path", "topological-inventory")
          host_sync_object = described_class.new(params_hash, "", "", "", 0)
          expect(host_sync_object.send(:build_topological_inventory_url_from, {})).to eq("http://example.com:8080/also/a/path/topological-inventory/v3.0")
        end
      end
    end
  end

  context "#host_inventory_api (private)" do
    it "returns the initial url if provided" do
      expect(
        described_class.send(:build_host_inventory_url, "http://example.com/api/", "")
      ).to eq("http://example.com/inventory/v1")
    end

    context "with service env vars set" do
      it "returns a sane value" do
        expect(
          described_class.send(:build_host_inventory_url, "http://example.com:8080", "")
        ).to eq("http://example.com:8080/inventory/v1")
      end

      it "uses the PATH_PREFIX with a leading slash" do
        expect(
          described_class.send(:build_host_inventory_url, "http://example.com:8080", "/this/is/a/path")
        ).to eq("http://example.com:8080/this/is/a/path/inventory/v1")
      end

      it "uses the PATH_PREFIX without a leading slash" do
        expect(
          described_class.send(:build_host_inventory_url, "http://example.com:8080", "also/a/path")
        ).to eq("http://example.com:8080/also/a/path/inventory/v1")
      end
    end
  end

  context "#process_message" do
    around { |spec| Timecop.freeze { spec.run } }

    let(:message) do
      OpenStruct.new(
        :payload => {
          "external_tenant" => account_number,
          "source"          => source,
          "payload"         => {
            "vms" => {
              "updated" => [{"id" => 1}, {"id" => 2}],
              "created" => [{"id" => 3}],
              "deleted" => [{"id" => 4}, {"id" => 5}],
            }
          }
        }
      )
    end

    let(:account_number) { "external_tenant_uuid" }
    let(:source) { "source_uuid" }

    let(:host_inventory_sync) do
      described_class.new(
        "http://mock/api/", "http://mock/api/", "localhost", 9092, 0)
    end

    let(:mac_addresses_1) { ["06:d5:e7:4e:c8:01", "06:d5:e7:4e:c7:01"] }
    let(:mac_addresses_2) { ["06:d5:e7:4e:c8:02"] }
    let(:mac_addresses_3) { ["06:d5:e7:4e:c8:03"] }
    let(:mac_addresses_5) { ["06:d5:e7:4e:c8:04"] }

    it "sends new hosts for create" do
      host_inventory_sync_service = host_inventory_sync
      logger                      = double
      allow(host_inventory_sync_service).to receive(:logger).and_return(logger)
      allow(logger).to receive(:info).exactly(7).times

      expect(host_inventory_sync_service).to(
        receive(:get_topological_inventory_vms)
          .with([1, 2, 3, 4, 5], "eyJpZGVudGl0eSI6eyJhY2NvdW50X251bWJlciI6ImV4dGVybmFsX3RlbmFu\ndF91dWlkIn19\n")
          .and_return(
            [
              {"id" => "1", "source_ref" => "vm1", "mac_addresses" => mac_addresses_1},
              {"id" => "2", "source_ref" => "vm2", "mac_addresses" => mac_addresses_2, "host_inventory_uuid" => ""},
              {"id" => "3", "source_ref" => "vm3", "mac_addresses" => mac_addresses_3, "host_inventory_uuid" => nil},
              {"id" => "4", "source_ref" => "vm4", "mac_addresses" => []},
              {"id" => "5", "source_ref" => "vm5", "mac_addresses" => mac_addresses_5, "host_inventory_uuid" => "host_uuid_5"},
            ]
          )
      )

      expect(host_inventory_sync_service).to(
        receive(:create_host_inventory_hosts)
          .with(*make_host_arg(mac_addresses_1, "vm1"))
          .and_return(
            mock_body({"data" => [{"host" => {"id" => "host_uuid_1"}}]})
          )
      )

      expect(host_inventory_sync_service).to(
        receive(:create_host_inventory_hosts)
          .with(*make_host_arg(mac_addresses_2, "vm2"))
          .and_return(
            mock_body({"data" => [{"host" => {"id" => "host_uuid_2"}}]})
          )
      )

      expect(host_inventory_sync_service).to(
        receive(:create_host_inventory_hosts)
          .with(*make_host_arg(mac_addresses_3, "vm3"))
          .and_return(
            mock_body({"data" => [{"host" => {"id" => "host_uuid_3"}}]})
          )
      )

      expect(host_inventory_sync_service).to(
        receive(:create_host_inventory_hosts)
          .with(*make_host_arg([], "vm4"))
          .and_return(
            mock_body({"data" => [{"host" => {"id" => "host_uuid_4"}}]})
          )
      )

      expect(host_inventory_sync_service).to(
        receive(:save_vms_to_topological_inventory).with(
          [
            TopologicalInventoryIngressApiClient::Vm.new(:source_ref => "vm1", :host_inventory_uuid => "host_uuid_1"),
            TopologicalInventoryIngressApiClient::Vm.new(:source_ref => "vm2", :host_inventory_uuid => "host_uuid_2"),
            TopologicalInventoryIngressApiClient::Vm.new(:source_ref => "vm3", :host_inventory_uuid => "host_uuid_3"),
            TopologicalInventoryIngressApiClient::Vm.new(:source_ref => "vm4", :host_inventory_uuid => "host_uuid_4"),
          ],
          source
        )
      )

      host_inventory_sync_service.send(:perform, message)
    end

    it "skips processing when no VMs are found" do
      host_inventory_sync_service = host_inventory_sync
      logger                      = double
      allow(host_inventory_sync_service).to receive(:logger).and_return(logger)
      allow(logger).to receive(:info).exactly(5).times

      message = OpenStruct.new(
        :payload => {
          "external_tenant" => account_number,
          "source"          => source,
          "payload"         => {
          }
        }
      )

      expect(host_inventory_sync_service.send(:perform, message)).to be_nil
    end

    it "skips processing when external tenant is missing" do
      host_inventory_sync_service = host_inventory_sync
      logger                      = double
      allow(host_inventory_sync_service).to receive(:logger).and_return(logger)
      allow(logger).to receive(:info).exactly(3).times
      allow(logger).to receive(:error).with(/Skipping payload because of missing :external_tenant/)

      message = OpenStruct.new(
        :payload => {
          "source"  => source,
          "payload" => {
          }
        }
      )

      expect(host_inventory_sync_service.send(:perform, message)).to be_nil
    end
  end

  def make_host_arg(mac_addresses, source_ref)
    [
      "eyJpZGVudGl0eSI6eyJhY2NvdW50X251bWJlciI6ImV4dGVybmFsX3RlbmFu\ndF91dWlkIn19\n",
      {
        :mac_addresses   => mac_addresses,
        :account         => account_number,
        :external_id     => source_ref,
        :display_name    => nil,
        :reporter        => "topological-inventory",
        :stale_timestamp => Time.now.utc + 86400
      }
    ]
  end

  def mock_body(body)
    OpenStruct.new(
      :body => body.to_json
    )
  end
end
