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
require 'fileutils'
require 'digest'

module LogStash
  module Inputs
    module CloudStorage
      # ProcessedDb tracks files and generations that have already been processed.
      # File names and generations are concatenated then SHA1 hashed.
      # The directory structure is git-like the first 3 characters of the hash are used
      # as a top level directory, and the rest is stored as a directory name within that.
      # This keeps the directory count manageable.
      class ProcessedDb
        def initialize(db_directory)
          @db_directory = db_directory
        end

        def already_processed?(blob)
          path = encode_path(blob)
          ::File.exist?(path)
        end

        def mark_processed(blob)
          path = encode_path(blob)
          FileUtils.mkdir_p(path)
        end

        def encode_path(blob)
          key = "#{blob.generation}|#{blob.name}"
          encoded = Digest::SHA1.hexdigest(key)
          prefix = encoded.slice(0, 3)
          suffix = encoded.slice(3..-1)

          ::File.join(@db_directory, prefix, suffix)
        end
      end
    end
  end
end
