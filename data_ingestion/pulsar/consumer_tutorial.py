#!/usr/bin/env python
#
# Copyright 2017 Streamlio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pulsar

# Create a Pulsar client instance. The instance can be shared across multiple
# producers and consumers
client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe('my-topic', 'my-subscription')

while True:
    msg = consumer.receive()
    print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
    consumer.acknowledge(msg)

client.close()

# while True:
#     try:
#         # try and receive messages with a timeout of 10 seconds
#         msg = consumer.receive(timeout_millis=10000)
#
#         # Do something with the message
#         print("Received message '%s'", msg.data())
#
#         # Acknowledge processing of message so that it can be deleted
#         consumer.acknowledge(msg)
#     except Exception:
#         print("No message received in the last 10 seconds")
#
# client.close()