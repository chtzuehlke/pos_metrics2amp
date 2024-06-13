#!/opt/homebrew/opt/ruby/bin/ruby

# FIXME below: make sure gems and dependencies are installed

require "net/http"
require "uri"
require "aws-sigv4"
require "google/protobuf"
require "faraday"
require "snappy"

descriptor_data = "\n\x17prom/remote_write.proto\";\n\x0cWriteRequest\x12\x1f\n\ntimeseries\x18\x01 \x03(\x0b\x32\x0b.TimeSeriesJ\x04\x08\x02\x10\x03J\x04\x08\x03\x10\x04\">\n\nTimeSeries\x12\x16\n\x06labels\x18\x01 \x03(\x0b\x32\x06.Label\x12\x18\n\x07samples\x18\x02 \x03(\x0b\x32\x07.Sample\"$\n\x05Label\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\r\n\x05value\x18\x02 \x02(\t\"*\n\x06Sample\x12\r\n\x05value\x18\x01 \x02(\x01\x12\x11\n\ttimestamp\x18\x02 \x02(\x03"

pool = Google::Protobuf::DescriptorPool.generated_pool

begin
  pool.add_serialized_file(descriptor_data)
rescue TypeError => e
  # Compatibility code: will be removed in the next major version.
  require "google/protobuf/descriptor_pb"
  parsed = Google::Protobuf::FileDescriptorProto.decode(descriptor_data)
  parsed.clear_dependency
  serialized = parsed.class.encode(parsed)
  file = pool.add_serialized_file(serialized)
  warn "Warning: Protobuf detected an import path issue while loading generated file #{__FILE__}"
  imports = []
  imports.each do |type_name, expected_filename|
    import_file = pool.lookup(type_name).file_descriptor
    if import_file.name != expected_filename
      warn "- #{file.name} imports #{expected_filename}, but that import was loaded as #{import_file.name}"
    end
  end
  warn "Each proto file must use a consistent fully-qualified name."
  warn "This will become an error in the next major version."
end

WriteRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("WriteRequest").msgclass
TimeSeries = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("TimeSeries").msgclass
Label = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("Label").msgclass
Sample = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("Sample").msgclass

aws_access_key_id = "FIXME" # FIXME Create an AWS IAM technical user with sufficient permissions to ingest metrics into AMP
aws_secret_access_key = "FIXME"
aws_region = "eu-west-1"

@prometheus_endpoint = "https://aps-workspaces.eu-west-1.amazonaws.com/workspaces/FIXME/api/v1/remote_write" # FIXME Create AMP workspace and configure AMP remote write endpoint

@signer = Aws::Sigv4::Signer.new(
  service: "aps",
  region: aws_region,
  access_key_id: aws_access_key_id,
  secret_access_key: aws_secret_access_key,
)

urls = [
  URI.parse("http://localhost:8181/") # FIXME Configure list of OpenMetrics-like endpoints to be scraped
]

loop do
  w = WriteRequest.new

  urls.each do |url|
    begin
      Net::HTTP.start(url.host, url.port, use_ssl: url.scheme == "https") do |http|
        http.open_timeout = 10
        http.read_timeout = 10

        request = Net::HTTP::Get.new(url)
        response = http.request(request)
        if response.code == "200"

          last_stamp = nil
          response.body.split("\n").each do |line|
            if line =~ /([A-Za-z_]+)\{([A-Za-z_]+)="(.+)"\} ([0-9\.]+)( [0-9\.]+)?/
              s = TimeSeries.new

              name = $1
              lkey = $2
              lvalue = $3
              value = $4
              stamp = $5.nil? ? nil : $5.strip

              puts "name=#{name},lkey=#{lkey},name=#{lvalue},name=#{lvalue},value=#{value},stamp=#{stamp}"

              s.labels.push Label.new(name: "__name__", value: name)
              s.labels.push Label.new(name: lkey, value: lvalue)

              if stamp
                last_stamp = (stamp.to_f * 1000).to_i
              end
              raise unless last_stamp

              s.samples.push Sample.new(timestamp: last_stamp, value: value.to_f)

              w.timeseries.push s
            end
          end
          
        end
      end
    rescue Errno::ECONNREFUSED => e
      puts "Warn: Connection Refused - #{e.message}"
    rescue Net::OpenTimeout => e
      puts "Warn: Opening Connection Timeout - #{e.message}"
    rescue Net::ReadTimeout => e
      puts "Warn: Reading Response Timeout - #{e.message}"
    rescue => e
      # FIXME raise e
      puts "Warn: Reading Response Error - #{e.message}"
    end
  end

  begin
    w_proto_snappy = Snappy.deflate(w.to_proto)

    headers = @signer.sign_request(
      http_method: "POST",
      url: @prometheus_endpoint,
      body: w_proto_snappy,
    ).headers

    response = Faraday.post @prometheus_endpoint, w_proto_snappy, headers

    puts response.status
    puts response.body
  rescue => e
    # FIXME raise e
    puts "Warn: AMP ingest Error - #{e.message}"
  end

  sleep(55)
end
