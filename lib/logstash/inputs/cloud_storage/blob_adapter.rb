# encoding: utf-8

# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
require 'java'
require 'logstash-input-google_cloud_storage_jars.rb'

module LogStash
  module Inputs
    module CloudStorage
      # BlobAdapter exposes parts of `com.google.cloud.storage.Blob` for use in
      # the plugin for easier mocking and future-proofing.
      class BlobAdapter
        def initialize(blob)
          @blob = blob
        end

        def name
          @blob.getName()
        end

        def attributes
          {
            'bucket' => @blob.getBucket(),
            'metadata' => @blob.getMetadata(),
            'name' => @blob.getName(),
            'md5' => @blob.getMd5(),
            'crc32c' => @blob.getCrc32c(),
            'generation' => @blob.getGeneration()
          }
        end

        java_import 'com.google.cloud.storage.Blob$BlobSourceOption'
        def delete!
          @blob.delete(BlobSourceOption.generationMatch())
        end

        def set_metadata!(key, value)
          new_metadata = { key => value }
          @blob.toBuilder().setMetadata(new_metadata).build().update()
        end

        def metadata
          @blob.getMetadata()
        end

        def generation
          @blob.getGeneration()
        end

        def line_attributes(line_number)
          attrs = attributes

          attrs['line'] = line_number
          attrs['line_id'] = "#{uri}:#{line_number}@#{generation}"

          attrs
        end

        def uri
          "gs://#{@blob.getBucket()}/#{name}"
        end

        java_import 'java.nio.file.Paths'
        def download_to(path)
          temp_path = Paths.get(path)
          @blob.downloadTo(temp_path)
        end

        def with_downloaded(temp_directory)
          temp_file = ::File.join(temp_directory, SecureRandom.uuid)
          download_to(temp_file)

          yield temp_file

          FileUtils.remove_entry_secure(temp_file, true)
        end
      end
    end
  end
end
