require "json"
require "topological_inventory/sync/worker"
require "topological_inventory/sync/inventory_upload/parser"
require "topological_inventory-ingress_api-client"
require "topological_inventory-ingress_api-client/save_inventory/saver"

module TopologicalInventory
  class Sync
    module InventoryUpload
      class ProcessorWorker < Worker
        include Logging

        def worker_name
          "Topological Inventory Insights Upload Processor Worker"
        end

        def queue_name
          "platform.upload.available"
        end

        def persist_ref
          "topological-inventory-upload-processor"
        end

        def perform(message)
          payload = JSON.parse(message.payload)
          return unless payload["service"] == "topological-inventory"

          account, request_id, payload_id = payload.values_at("account", "request_id", "payload_id")
          log_header = "account [#{account}] request_id [#{request_id}]"

          logger.info("#{log_header}: Processing payload [#{payload_id}]...")

          Parser.parse_inventory_payload(payload['url']) do |inventory|
            process_inventory(inventory, account)
          end

          logger.info("#{log_header}: Processing payload [#{payload_id}]...Complete")
        end

        private

        # @param inventory [Hash]
        # @param account [String] account from x-rh-identity header
        def process_inventory(inventory, account)
          send("process_#{payload_type(inventory)}_inventory", inventory, account)
        end

        def process_default_inventory(inventory, account)
          inventory = TopologicalInventoryIngressApiClient::Inventory.new.build_from_hash(inventory.deep_symbolize_keys)
          _source = process_source(account, inventory.source_type, inventory.name, inventory.source)
          send_to_ingress_api(inventory)
        end

        def process_cfme_inventory(inventory, account)
          cfme_ems_types.each do |ems_type|
            inventory[ems_type].to_a.each do |payload|
              payload = process_cfme_provider_inventory(ems_type, payload, account)
              send_to_ingress_api(payload)
            end
          end
        end

        def process_cfme_provider_inventory(ems_type, payload, account)
          source_type = ems_type_to_source_type(ems_type)
          source_uid  = payload["guid"]
          source_name = payload["name"]

          _source = process_source(account, source_type, source_name, source_uid)

          inventory = TopologicalInventoryIngressApiClient::Inventory.new(
            :source                  => source_uid,
            :source_type             => source_type,
            :schema                  => TopologicalInventoryIngressApiClient::Schema.new(:name => "Default"),
            :name                    => source_name,
            :refresh_state_uuid      => SecureRandom.uuid,
            :refresh_state_part_uuid => SecureRandom.uuid,
            :collections             => [],
          )

          if payload["ems_clusters"].present?
            clusters_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "clusters", :data => [])
            payload["ems_clusters"].each do |cluster_data|
              clusters_collection.data << TopologicalInventoryIngressApiClient::Cluster.new(
                :name       => cluster_data["name"],
                :source_ref => cluster_data["ems_ref"],
                :uid_ems    => cluster_data["uid_ems"],
                :extra      => {
                  :ha_enabled       => cluster_data["ha_enabled"],
                  :drs_enabled      => cluster_data["drs_enabled"],
                  :effective_cpu    => cluster_data["effective_cpu"],
                  :effective_memory => cluster_data["effective_memory"]
                }
              )
            end
            inventory.collections << clusters_collection
          end

          if payload["hosts"].present?
            hosts_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "hosts", :data => [])

            payload["hosts"].each do |host_data|
              memory_mb = host_data.dig("hardware", "memory_mb")
              cluster_ref = host_data.dig("ems_cluster", "ems_ref")
              cluster     = TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                :inventory_collection_name => "clusters", :reference => {:source_ref => cluster_ref}, :ref => :manager_ref
              ) unless cluster_ref.nil?

              hosts_collection.data << TopologicalInventoryIngressApiClient::Host.new(
                :name        => host_data["name"],
                :hostname    => host_data["hostname"],
                :ipaddress   => host_data["ipaddress"],
                :power_state => host_data["power_state"],
                :uid_ems     => host_data["uid_ems"],
                :source_ref  => host_data["ems_ref"],
                :cpus        => host_data["cpu_total_cores"],
                :memory      => memory_mb * 1048576,
                :cluster     => cluster,
                :extra       => {
                  :cpu_cores_per_socket => host_data["cpu_cores_per_socket"],
                  :maintenance          => host_data["maintenance"],
                  :vmm_vendor           => host_data["vmm_vendor"],
                  :vmm_version          => host_data["vmm_version"],
                  :vmm_product          => host_data["vmm_product"],
                  :vmm_buildnumber      => host_data["vmm_buildnumber"],
                }
              )
            end

            inventory.collections << hosts_collection
          end

          if payload["storages"].present?
            datastores_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "datastores", :data => [])
            datastore_mounts_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "datastore_mounts", :data => [])

            payload["storages"].each do |storage_data|
              datastore_data = {
                :name        => storage_data["name"],
                :location    => storage_data["location"],
                :total_space => storage_data["total_space"],
                :free_space  => storage_data["free_space"],
                :extra       => {
                  :uncommitted         => storage_data["uncommitted"],
                  :storage_domain_type => storage_data["storage_domain_type"]
                }
              }

              storage_data["host_storages"].group_by { |hs| hs["ems_ref"] }.each do |ems_ref, host_storages|
                datastores_collection.data << TopologicalInventoryIngressApiClient::Datastore.new(
                  datastore_data.merge(:source_ref => ems_ref)
                )

                host_storages.each do |host_storage|
                  host_ref = host_storage.dig("host", "ems_ref")
                  next if host_ref.nil?

                  datastore_mounts_collection.data << TopologicalInventoryIngressApiClient::DatastoreMount.new(
                    :datastore => TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                      :inventory_collection_name => "datastores", :reference => {:source_ref => ems_ref}, :ref => :manager_ref
                    ),
                    :host      => TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                      :inventory_collection_name => "hosts", :reference => {:source_ref => host_ref}, :ref => :manager_ref
                    ),
                  )
                end
              end
            end

            inventory.collections << datastores_collection
            inventory.collections << datastore_mounts_collection
          end

          if payload["vms"].present?
            vms_collection = TopologicalInventoryIngressApiClient::InventoryCollection.new(:name => "vms", :data => [])
            payload["vms"].each do |vm_data|
              host_ref = vm_data.dig("host", "ems_ref")
              host     = TopologicalInventoryIngressApiClient::InventoryObjectLazy.new(
                :inventory_collection_name => "hosts", :reference => {:source_ref => host_ref}, :ref => :manager_ref
              ) unless host_ref.nil?

              vms_collection.data << TopologicalInventoryIngressApiClient::Vm.new(
                :name        => vm_data["name"],
                :description => vm_data["description"],
                :cpus        => vm_data["cpu_total_cores"],
                :memory      => vm_data["ram_size_in_bytes"],
                :source_ref  => vm_data["ems_ref"],
                :uid_ems     => vm_data["uid_ems"],
                :power_state => vm_data["power_state"],
                :host        => host,
                :extra       => {
                  :cpu_cores_per_socket => vm_data["cpu_cores_per_socket"],
                  :disks_aligned        => vm_data["disks_aligned"],
                  :has_rdm_disk         => vm_data["has_rdm_disk"],
                  :linked_clone         => vm_data["linked_clone"],
                  :retired              => vm_data["retired"],
                  :v_datastore_path     => vm_data["v_datastore_path"],
                }
              )
            end
            inventory.collections << vms_collection
          end

          inventory
        end

        def ems_type_to_source_type(ems_type)
          case ems_type
          when "ManageIQ::Providers::OpenStack::CloudManager"
            "openstack"
          when "ManageIQ::Providers::Redhat::InfraManager"
            "rhv"
          when "ManageIQ::Providers::Vmware::InfraManager"
            "vsphere"
          else
            raise "Invalid provider type #{ems_type}"
          end
        end

        def cfme_ems_types
          [
            "ManageIQ::Providers::OpenStack::CloudManager",
            "ManageIQ::Providers::Redhat::InfraManager",
            "ManageIQ::Providers::Vmware::InfraManager"
          ]
        end

        def process_source(account, source_type, source_name, source_uid)
          sources_api = sources_api_client(account)
          source_type = find_source_type(source_type, sources_api)

          find_or_create_source(sources_api, source_type.id, source_name, source_uid)
        end

        def payload_type(inventory)
          inventory.dig("schema", "name").downcase
        end

        def find_source_type(source_type_name, sources_api)
          raise 'Missing Source Type name!' if source_type_name.blank?

          response = sources_api.list_source_types({:filter => {:name => source_type_name}})
          if response.data.blank?
            raise "Source Type #{source_type_name} not found!"
          else
            logger.info("Source Type #{source_type_name} found")
            response.data.first
          end
        end

        def find_or_create_source(sources_api, source_type_id, source_name, source_uid)
          return if source_name.nil?

          sources = sources_api.list_sources({:filter => {:uid => source_uid}})

          if sources.data.blank?
            source = SourcesApiClient::Source.new(:uid => source_uid, :name => source_name, :source_type_id => source_type_id)
            source, status_code, _ = sources_api.create_source_with_http_info(source)

            if status_code == 201
              logger.info("Source #{source_name}(#{source_uid}) created successfully")
              source
            else
              raise "Failed to create Source #{source_name} (#{source_uid})"
            end
          else
            logger.debug("Source #{source_name} (#{source_uid}) found")
            sources.data.first
          end
        end

        # TODO: Now it handles only "Default" schema
        def convert_to_topological_inventory_schema(inventory)
          inventory
        end

        def send_to_ingress_api(inventory)
          logger.info("[START] Send to Ingress API with :refresh_state_uuid => '#{inventory.refresh_state_uuid}'...")

          sender = ingress_api_sender

          # Send data to ingress_api
          total_parts = sender.save(:inventory => inventory)

          # Send total parts sent to ingress_api
          sender.save(
            :inventory => inventory_for_sweep(inventory, total_parts)
          )

          logger.info("[COMPLETED] Send to Ingress API with :refresh_state_uuid => '#{inventory.refresh_state_uuid}'. Total parts: #{total_parts}")
          total_parts
        end

        def inventory_for_sweep(inventory, total_parts)
          TopologicalInventoryIngressApiClient::Inventory.new(
            :name => inventory.name,
            :schema => TopologicalInventoryIngressApiClient::Schema.new(:name => inventory.schema.name),
            :source => inventory.source,
            :collections => [],
            :refresh_state_uuid => inventory.refresh_state_uuid,
            :total_parts => total_parts,
            :sweep_scope => inventory.collections.collect { |collection| collection.name }.compact
          )
        end

        def ingress_api_client
          TopologicalInventoryIngressApiClient::DefaultApi.new
        end

        def ingress_api_sender
          TopologicalInventoryIngressApiClient::SaveInventory::Saver.new(
            :client => ingress_api_client,
            :logger => logger
          )
        end
      end
    end
  end
end
