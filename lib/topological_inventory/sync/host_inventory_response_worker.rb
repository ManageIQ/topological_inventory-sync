require "concurrent"
require "stringio"

module TopologicalInventory
  class Sync
    class ResponseWorker
      attr_reader :started
      alias started? started

      def initialize(config, logger, metrics)
        self.config              = config
        self.lock                = Mutex.new
        self.logger              = logger
        self.metrics             = metrics
        self.registered_messages = Concurrent::Map.new
        self.started             = Concurrent::AtomicBoolean.new(false)
        self.timeout_lock        = Mutex.new
        self.workers             = {}
      end

      # Start listening on Kafka
      def start
        lock.synchronize do
          return if started.value

          started.value = true
          workers[:maintenance] = Thread.new { check_timeouts while started.value }
          workers[:listener]    = Thread.new { listen while started.value }
        end
      end

      # Stop listener
      def stop
        lock.synchronize do
          return unless started.value

          started.value = false
          workers[:listener]&.terminate
          workers[:maintenance]&.join
        end
      end

      def register_message(external_id, source_id)
        registered_messages[external_id] = {
          :source_id  => source_id,
          :created_at => Time.now.utc
        }
      end

      private

      attr_accessor :config, :lock, :timeout_lock, :metrics, :logger, :workers, :registered_messages
      attr_writer :started

      def listen
        # Open a connection to the messaging service
        client = ManageIQ::Messaging::Client.open(default_messaging_opts)

        logger.info("Topological Inventory Sync Response worker started...")
        client.subscribe_topic(queue_opts) do |message|
          process_message(message)
        end
      rescue => err
        logger.error("Exception in kafka listener: #{err}\n#{err.backtrace.join("\n")}")
        metrics.record_error(:response)
      ensure
        client&.close
      end

      def process_message(message)
        response = JSON.parse(message.payload)

        return unless response['type'] == 'created'

        host           = response['host'] if response
        external_id    = host['external_id'] if host
        registered_msg = registered_messages[external_id] if external_id
        source_id      = registered_msg[:source_id] if registered_msg

        if source_id.nil?
          logger.debug("ResponseWorker: There is no registered request for '#{host['display_name']}' VM with external_id:'#{external_id}'")
          return
        end

        new_vm = TopologicalInventoryIngressApiClient::Vm.new(
          :source_ref          => external_id,
          :host_inventory_uuid => host["id"]
        )

        save_vms_to_topological_inventory([new_vm], source_id)
        registered_messages.delete(external_id)
        logger.info("Topological Inventory has been updated with '#{host['display_name']}' VM (source:'#{source_id}').")
      rescue => e
        logger.error("#{e.message} -  #{e.backtrace.join("\n")}")
        metrics&.record_error(:response)
      end

      def check_timeouts(threshold = config.response_timeout)
        expired = []
        #
        # STEP 1 Collect expired messages
        #
        registered_messages.each do |external_id, msg|
          timeout_lock.synchronize do
            if msg[:created_at] < Time.now.utc - threshold
              expired << external_id
            end
          end
        end

        #
        # STEP 2 Remove expired messages, send timeout callbacks
        #
        expired.each do |external_id|
          registered_messages.delete(external_id)
          logger.debug("ResponseWorker: Registered request for external_id = '#{external_id}' has reached the timeout and it has been removed.")
          metrics&.record_error(:response_timeout)
        end

        sleep(config.response_timeout_poll_time)
      rescue => err
        logger.error("Exception in maintenance worker: #{err}\n#{err.backtrace.join("\n")}")
        metrics&.record_error(:response)
      end

      def save_vms_to_topological_inventory(topological_inventory_vms, source)
        # TODO(lsmola) if VM will have subcollections, this will need to send just partial data, otherwise all subcollections
        # would get deleted. Alternative is having another endpoint than :vms, for doing update only operation. Partial data
        # are not supported without timestamp for update (at least for now), since that is just being added to skeletal
        # precreate.
        TopologicalInventory::Providers::Common::SaveInventory::Saver.new(:client => ingress_api_client, :logger => logger).save(
          :inventory => TopologicalInventoryIngressApiClient::Inventory.new(
            :schema                     => TopologicalInventoryIngressApiClient::Schema.new(:name => "Default"),
            :source                     => source,
            :collections                => [
              TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => :vms, :data => topological_inventory_vms)
            ],
            :refresh_state_part_sent_at => Time.now.utc
          )
        )
      end

      def ingress_api_client
        TopologicalInventoryIngressApiClient::DefaultApi.new
      end

      # No persist_ref here, because all instances (pods) needs to receive kafka message
      def queue_opts
        opts               = {:service => config.queue_topic}
        opts[:auto_ack]    = config.queue_auto_ack    unless config.queue_auto_ack.nil?
        opts[:max_bytes]   = config.queue_max_bytes   if config.queue_max_bytes
        opts[:persist_ref] = config.queue_persist_ref if config.queue_persist_ref
        opts
      end

      def default_messaging_opts
        {
          :host       => config.queue_host,
          :port       => config.queue_port,
          :protocol   => :Kafka,
          :client_ref => ENV['HOSTNAME'], # A reference string to identify the client
        }
      end
    end
  end
end
