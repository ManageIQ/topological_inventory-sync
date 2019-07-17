module TopologicalInventory
  class Sync
    module InventoryUpload
      class Parser
        class << self
          def parse_inventory_payload(url)
            open_url(url) do |io|
              untargz(io) do |file|
                require "json/stream"
                yield JSON::Stream::Parser.parse(file)
              end
            end
          end

          private

          def untargz(io)
            require "rubygems/package"
            Zlib::GzipReader.wrap(io) do |gz|
              Gem::Package::TarReader.new(gz) do |tar|
                tar.each { |entry| yield entry }
              end
            end
          end

          def open_url(url)
            require "http"

            uri = URI(url)
            if ["file", nil].include?(uri.scheme)
              File.open(uri.path) { |f| yield f }
            else
              response = HTTP.get(uri)
              response.body.stream!
              yield response.body
            end
          end
        end
      end
    end
  end
end
