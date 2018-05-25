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

require 'logstash/devutils/rspec/spec_helper'
require 'logstash/inputs/google_cloud_storage'
require 'logstash/inputs/cloud_storage/client'

describe LogStash::Inputs::CloudStorage::Client do

  # This test is mostly to make sure the Java types, signatures and classes
  # haven't changed being that JRuby is very relaxed.
  describe '#initialize' do
    let(:logger) { spy('logger') }

    it 'does not throw an error when initializing' do
      key_file = ::File.join('spec', 'fixtures', 'credentials.json')
      LogStash::Inputs::CloudStorage::Client.new('my-bucket', key_file, logger)
    end
  end
end