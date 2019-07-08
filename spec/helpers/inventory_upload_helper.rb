module InventoryUploadHelper
  def targz(payload)
    require "rubygems/package"

    file = StringIO.new("", "w")
    Zlib::GzipWriter.wrap(file) do |gz|
      Gem::Package::TarWriter.new(gz) do |tar|
        payload_json = payload.to_json
        tar.add_file_simple("inventory.json", 0444, payload_json.length) do |io|
          io.write(payload_json)
        end
      end
    end

    StringIO.new(file.string, "r")
  end

  def ocp_inventory(source = "90df364e-b82b-4617-91c5-98c3f3518544")
    sample_inventory('OCP', 'openshift', source)
  end

  def sample_inventory(name, source_type, source, schema = 'Default')
    {
      'name'        => name,
      'schema'      => {'name' => schema},
      'source'      => source,
      'source_type' => source_type,
      'collections' => []
    }
  end

  def insights_upload_payload
    "{\"account\":\"12345\",\"rh_account\":\"12345\",\"principal\":\"54321\",\"request_id\":\"52df9f748eabcfeb\",\
    \"payload_id\":\"52df9f748eabcfeb\",\"size\":458,\"service\":\"topological-inventory\",\"category\":\"something\",\
    \"b64_identity\":\"eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0=\",\
    \"url\":\"/tmp/upload/schema.tar.gz\"}"
  end
end
