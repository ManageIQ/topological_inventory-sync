require "topological_inventory/sync"
require "sources-api-client"

RSpec.describe TopologicalInventory::Sync::Iterator do
  let(:app_types_count) { 4 }
  let(:app_types) do
    data = []
    app_types_count.times do |i|
      data << SourcesApiClient::ApplicationType.new(
        :name => "insights/platform/app-#{i}",
        :dependent_applications => [],
        :id => i.to_s
      )
    end
    data
  end

  let(:sources_api_client) { double("SourcesApiClient::Default") }

  describe "#each" do
    context "without resource" do
      it "calls the block and yields returned data" do
        response = SourcesApiClient::ApplicationTypesCollection.new(
          :data => [app_types[0]],
          :meta => SourcesApiClient::CollectionMetadata.new(:count => 1)
        )
        expect(sources_api_client).to receive(:list_application_types).and_return(response)

        iterator = described_class.new(lambda {|_, _| sources_api_client.list_application_types})

        cnt = 0
        iterator.each do |data|
          expect(data).to eq(app_types[0])
          cnt += 1
        end
        expect(cnt).to eq(1)
      end

      (1..5).each do |limit|
        it "calls the block with pagination (limit: #{limit})" do
          offset = 0
          while offset < app_types_count do
            low, high = offset, [offset + limit - 1, app_types_count].min
            response = SourcesApiClient::ApplicationTypesCollection.new(
              :data => app_types[low..high],
              :meta => SourcesApiClient::CollectionMetadata.new(:count => app_types_count)
            )
            expect(sources_api_client).to(receive(:list_application_types)
                                            .with(:limit => limit, :offset => offset)
                                            .and_return(response))
            offset += limit
          end
          block = lambda { |l, o| sources_api_client.list_application_types(:limit => l, :offset => o)}
          iterator = described_class.new(block, :limit => limit)

          cnt = 0
          iterator.each do |data|
            expect(data).to eq(app_types[cnt])
            cnt += 1
          end

          expect(cnt).to eq(app_types_count)
        end
      end

      it "raises an exception if response isn't compatible with api client" do
        response = {:data => app_types}
        expect(sources_api_client).to receive(:unexpected_request).and_return(response)

        iterator = described_class.new(lambda {|_, _| sources_api_client.unexpected_request})
        msg      = "Provided block expects Sources API Client response"
        expect { iterator.each { |_| expect("It shouldn't be here").to be_falsey } }.to raise_exception(msg)
      end
    end

    context "with resource" do
      let(:application) { SourcesApiClient::Application.new(:id => '1', :application_type_id => '1', :source_id => '1')}

      it "calls the block with 3 parameters" do
        source_id = '10'
        response = SourcesApiClient::ApplicationsCollection.new(
          :data => [application],
          :meta => SourcesApiClient::CollectionMetadata.new(:count => 1)
        )
        expect(sources_api_client).to(receive(:list_source_applications)
                                        .with(source_id, :limit => 1, :offset => 0)
                                        .and_return(response))

        block = lambda { |src_id, l, o| sources_api_client.list_source_applications(src_id, :limit => l, :offset => o)}
        iterator = described_class.new(block, :limit => 1, :resource_id => source_id)

        cnt = 0
        iterator.each do |data|
          expect(data).to eq(application)
          cnt += 1
        end
        expect(cnt).to eq(1)
      end
    end
  end
end
