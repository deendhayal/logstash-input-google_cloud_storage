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
require 'zlib'

module LogStash
  module Inputs
    module CloudStorage
      # FileReader provides a unified way to read different types of log files
      # with predictable callbacks.
      class FileReader
        # read_lines reads lines from a file one at a time, optionally decoding
        # the file as gzip if decode_gzip is true.
        #
        # Handles files with both UNIX and Windows line endings.
        def self.read_lines(filename, decode_gzip, &block)
          if decode_gzip && gzip?(filename)
            read_gzip_lines(filename, &block)
          else
            read_plain_lines(filename, &block)
          end
        end

        # gzip? returns true if the given filename has a gzip file extension.
        def self.gzip?(filename)
          filename.end_with? '.gz'
        end

        def self.read_plain_lines(filename, &block)
          line_num = 1
          ::File.open(filename).each do |line|
            block.call(line.chomp, line_num)
            line_num += 1
          end
        end

        def self.read_gzip_lines(filename, &block)
          line_num = 1
          Zlib::GzipReader.open(filename).each_line do |line|
            block.call(line.chomp, line_num)
            line_num += 1
          end
        end
      end
    end
  end
end
