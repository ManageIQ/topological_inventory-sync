RSpec.describe TopologicalInventory::Sync::Metrics do
  context "[disabled]" do
    subject { described_class.new(0) }

    it "doesn't initialize an object if port is 0" do
      expect(PrometheusExporter::Server::WebServer).not_to receive(:new)
      expect(PrometheusExporter::LocalClient).not_to receive(:new)
      expect(PrometheusExporter::Instrumentation::Process).not_to receive(:start)
    end
  end

  context "[enabled] " do
    let(:errors_counter) { double('errors_counter') }

    before do
      allow_any_instance_of(described_class).to receive_messages(:configure_metrics => nil,
                                                                 :configure_server  => nil)

      @metrics = described_class.new
      @metrics.instance_variable_set(:@errors_counter, errors_counter)
    end

    describe "#record_error" do
      it "increases error counter" do
        expect(errors_counter).to receive(:observe).with(1)

        @metrics.record_error
      end
    end
  end
end
