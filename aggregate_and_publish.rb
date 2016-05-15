require 'uri' #for parsing urls
require 'net/http' #for downloading files
require 'nokogiri' #nokogiri gem for parsing html for file names
require 'zip/zip' #rubyzip gem for unzipping files
require 'redis' #redis gem for publishing to redis
require 'pp' #for pretty printing output

# Parameters
@url = "http://bitly.com/nuvi-plz"
@temp_directory = "tmp"
@redis_list = "NEWS_XML"

# Internal variables
@error = nil
@errors = []
@file_list = []
@uri = URI.parse(@url)
@redis = Redis.new

# Resolve url
def resolve_url(uri, limit = 10)
  # Break out if there's a chance we're stuck in an endless redirection
  if limit == 0
    @error = 'HTTP redirect too deep' 
    return nil
  end

  request = Net::HTTP::Get.new(uri.path)
  response = Net::HTTP.start(uri.host, uri.port) do |http|
    http.request(request)
  end
  case response
    when Net::HTTPSuccess then uri
    when Net::HTTPRedirection then resolve_url(URI.parse(response['location']), limit - 1)
    else
      @error = "Could not resolve url"
      return nil
  end
end

# Generate file list from url
def generate_file_list(uri)
  response = get_file_list(uri)
  return parse_file_list_response(response) unless has_error?
end

def get_file_list(uri)
  html = ""
  begin
    request = Net::HTTP::Get.new(uri.path)
    Net::HTTP.start(uri.host, uri.port) do |http|
      http.request(request) do |response|
        html = response.body
      end
    end
    @error = nil
    return html
  rescue Exception => e
    @error = "File list could not be generated"
  end
end

def parse_file_list_response(html)
  doc = Nokogiri::HTML(html)
  return doc.xpath("//tr/td/a/@href").collect{ |node| node.value }.select{ |filename| filename =~ /\.zip$/ }
end

# Skip previously-processed files
def file_already_processed?(file_name)
  # Assumes that no two zip files contain identical xml documents
  return @redis.hget("#{@redis_list}_META", file_name).eql?("1")
end

# Download file
def download_file(uri, file_name)
  f = open("#{@temp_directory}/#{file_name}", "w")
  begin
    Net::HTTP.start(uri.host, uri.port) do |http|
      http.get("#{uri.path}/#{file_name}") do |response|
        f.write(response)
      end
    end
    @error = nil
  rescue Exception => e
    @error = "File #{file_name} could not be downloaded"
  ensure
    f.close()
  end
end  

# Unzip file
def unzip_file(file_name)
  Zip::ZipFile.open("#{@temp_directory}/#{file_name}") do |zip_file|
    zip_file.each do |doc|
      file_path = File.join("#{@temp_directory}/#{file_name.gsub(/\.zip$/, '')}/#{doc.name}")
      FileUtils.mkdir_p(File.dirname(file_path))
      zip_file.extract(doc, file_path)
    end
  end
end

# Process file
def process_file(file_name)
  @redis.hset("#{@redis_list}_META", file_name, 1)
  Dir["#{@temp_directory}/#{file_name.gsub(/\.zip$/, '')}/*.xml"].each do |doc|
    # Consider hashing the file, and skipping over files that have identical hashes as what's been processed already

    file_contents = ""
    File.open(doc, "r") do |file|
      file_contents = file.read
    end
    @redis.pipelined do
      @redis.rpush(@redis_list, file_contents)
    end
  end
end

# Delete file
def delete_file(file_name)
  File.delete("#{@temp_directory}/#{file_name}")
  FileUtils.rm_r("#{@temp_directory}/#{file_name.gsub(/\.zip$/, '')}")
end

# Handle errors
def has_error?
  if !@error.nil?
    puts @error
    @errors << @error
    @error = nil
    return true
  else
    return false
  end
end

# Do it for all files
puts "Starting processing."
@uri = resolve_url(@uri)
@file_list = generate_file_list(@uri) unless has_error?

@file_list = @file_list[0...10]

@file_list.each do |file|
  next if file_already_processed?(file)
  download_file(@uri, file) 
  @errors << @error if @error
  @error = nil
  unzip_file(file)
  process_file(file)
  delete_file(file)
end unless has_error?
puts "Finished processing."

# Debugging output
puts "Errors: "
pp @errors
list_length = @redis.llen(@redis_list)
puts "List size: #{list_length}" 
pp @redis.hgetall("#{@redis_list}_META")
#pp @redis.lrange(@redis_list, 0, list_length)
