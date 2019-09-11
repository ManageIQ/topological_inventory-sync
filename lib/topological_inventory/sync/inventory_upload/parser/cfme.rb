module TopologicalInventory
  class Sync
    module InventoryUpload
      class Parser
        class Cfme < Parser
          def parse
            inventory = ingress_api_inventory

            clusters_collection = ingress_api_inventory_collection("clusters")
            payload["ems_clusters"].to_a.each do |cluster_data|
              clusters_collection.data << parse_cluster(cluster_data)
            end
            inventory.collections << clusters_collection

            hosts_collection = ingress_api_inventory_collection("hosts")
            payload["hosts"].to_a.each do |host_data|
              hosts_collection.data << parse_host(host_data)
            end
            inventory.collections << hosts_collection

            datastores_collection, datastore_mounts_collection = parse_storages(payload)

            inventory.collections << datastores_collection
            inventory.collections << datastore_mounts_collection

            vms_collection = ingress_api_inventory_collection("vms")
            payload["vms"].to_a.each do |vm_data|
              vms_collection.data << parse_vm(vm_data)
            end
            inventory.collections << vms_collection

            inventory
          end

          private

          def parse_cluster(cluster_data)
            TopologicalInventoryIngressApiClient::Cluster.new(
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

          def parse_host(host_data)
            memory_mb = host_data.dig("hardware", "memory_mb") || 0
            cluster_ref = host_data.dig("ems_cluster", "ems_ref")
            cluster     = ingress_api_lazy_ref("clusters", cluster_ref) unless cluster_ref.nil?

            TopologicalInventoryIngressApiClient::Host.new(
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

          def parse_storages(payload)
            datastores_collection       = ingress_api_inventory_collection("datastores")
            datastore_mounts_collection = ingress_api_inventory_collection( "datastore_mounts")

            payload["storages"].to_a.each do |storage_data|
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
                    :datastore => ingress_api_lazy_ref("datastores", ems_ref),
                    :host      => ingress_api_lazy_ref("hosts",      host_ref)
                  )
                end
              end
            end

            return datastores_collection, datastore_mounts_collection
          end

          def parse_vm(vm_data)
            host_ref = vm_data.dig("host", "ems_ref")
            host     = ingress_api_lazy_ref("hosts", host_ref) unless host_ref.nil?

            TopologicalInventoryIngressApiClient::Vm.new(
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
        end
      end
    end
  end
end
