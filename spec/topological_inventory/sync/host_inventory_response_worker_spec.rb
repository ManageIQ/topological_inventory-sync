require 'topological_inventory/sync/host_inventory_response_worker'
require 'topological_inventory-ingress_api-client'

RSpec.describe TopologicalInventory::Sync::ResponseWorker do
  let(:logger)      { double('logger') }
  let(:config)      { double('config') }
  let(:vm_name)     { 'vm-sync-test' }
  let(:source_id)   { 'source_id' }
  let(:external_id) do
    '/subscriptions/d8dab060-0000-0000-82d7-afc3ff5c7b06/resourceGroups' \
    "/test/providers/Microsoft.Compute/virtualMachines/#{vm_name}"
  end

  subject { described_class.new(config, logger) }

  describe '#register_message' do
    it 'saves external_id and related source_id' do
      subject.register_message(external_id, source_id)

      expect(subject.send(:registered_messages)[external_id][:source_id]).to eq(source_id)
    end
  end

  describe '#process_message' do
    let(:msg)         { Message.new(msg_type, vm_name, external_id, vm_id) }
    let(:msg_type)    { 'created' }
    let(:vm_id)       { 'd45bc3e0-0000-0000-0000-9dec543acad4' }

    let(:new_vm) do
      TopologicalInventoryIngressApiClient::Vm.new(
        :source_ref          => external_id,
        :host_inventory_uuid => vm_id
      )
    end

    it 'updates Topological Inventory' do
      subject.register_message(external_id, source_id)

      expect(subject).to receive(:save_vms_to_topological_inventory).with([new_vm], source_id)
      expect(logger).to receive(:info).with("Topological Inventory has been updated with '#{vm_name}' VM (source:'#{source_id}').")

      subject.send(:process_message, msg)
    end

    it 'skips without related source' do
      expect(logger).to receive(:debug).with(
        "ResponseWorker: There is no registered request for '#{vm_name}' VM with external_id:'#{external_id}'"
      )

      subject.send(:process_message, msg)
    end

    it "skips for any other type than 'create'" do
      msg.msg_type = 'updated'
      expect(subject).to_not receive(:save_vms_to_topological_inventory)

      subject.send(:process_message, msg)
    end

    it 'does not process message without payload' do
      msg_payload = 'Hello World!'
      expect(msg).to receive(:payload).and_return(msg_payload)
      expect(logger).to_not receive(:info)
      expect(logger).to receive(:error).with(
        /.*(unexpected token at 'Hello World!').*/
      )

      subject.send(:process_message, msg)
    end
  end

  describe '#check_timeouts' do
    before do
      allow(config).to receive(:response_timeout).and_return(2.minutes)
      allow(config).to receive(:response_timeout_poll_time).and_return(0.seconds)
    end

    it 'removes expired sources' do
      id     = external_id
      source = source_id
      subject.instance_eval do
        registered_messages[id] = {:source_id => source, :created_at => Time.now.utc - 1.hour}
      end

      expect(logger).to receive(:debug).with(
        "ResponseWorker: Registrated request for external_id = '#{id}' has reached the timeout and it has been removed."
      )

      subject.send(:check_timeouts)
      expect(subject.send(:registered_messages).size).to eq(0)
    end

    it 'does not remove valid sources' do
      subject.register_message(external_id, source_id)
      subject.send(:check_timeouts)
      expect(subject.send(:registered_messages)[external_id][:source_id]).to eq(source_id)
    end
  end
end

class Message
  attr_accessor :msg_type, :vm_name, :external_id, :vm_id

  def initialize(msg_type = 'created', vm_name = 'vm', external_id = 'external_id', vm_id = 'vm_id')
    self.msg_type    = msg_type
    self.vm_name     = vm_name
    self.external_id = external_id
    self.vm_id       = vm_id
  end

  def payload
    {
      'metadata'          => {'request_id' => '-1'},
      'type'              => msg_type,
      'host'              => {
        'insights_id'             => nil,
        'ip_addresses'            => nil,
        'id'                      => vm_id,
        'culled_timestamp'        => '2020-08-26T14:00:56+00:00',
        'account'                 => '6081020',
        'tags'                    => [],
        'display_name'            => vm_name,
        'created'                 => '2020-08-11T12:26:05.696381+00:00',
        'satellite_id'            => nil,
        'mac_addresses'           => ['00-22-48-23-D4-04'],
        'stale_warning_timestamp' => '2020-08-19T14:00:56+00:00',
        'stale_timestamp'         => '2020-08-12T14:00:56+00:00',
        'ansible_host'            => nil,
        'subscription_manager_id' => nil,
        'rhel_machine_id'         => nil,
        'external_id'             => external_id,
        'reporter'                => 'topological-inventory',
        'updated'                 => '2020-08-11T14:00:56.515374+00:00',
        'system_profile'          => {},
        'fqdn'                    => nil,
        'bios_uuid'               => nil
      },
      'platform_metadata' => {},
      'timestamp'         => '2020-08-11T14:00:56.521853+00:00'
    }.to_json
  end
end
