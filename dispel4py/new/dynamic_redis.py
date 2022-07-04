# Copyright (c) The University of Edinburgh 2014-2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Dynamic Using Redis.

refer to redis document: https://redis.io/docs/manual/data-types/streams
'''

import redis

# Redis group prefix
REDIS_STREAM_PREFIX = "DISPEL4PY_DYNAMIC_STREAM_"

jobid = "abcd1234"
# Redis group name
redis_stream_name = REDIS_STREAM_PREFIX + jobid
redis_stream_group_name = "QUEUE"

# connect to redis
r = redis.Redis(host='localhost', port=6379)

if r.exists(redis_stream_name):
    r.delete(redis_stream_name)

# create consumer group, read FIFO, auto create stream
r.xgroup_create(redis_stream_name,redis_stream_group_name,"$",True)

# create messages
message1 = {'peid': 11, "data": "aaa"}
message2 = {'peid': 22, "data": "bbb"}

# add message to Redis stream queue
message1_id = r.xadd(redis_stream_name, message1)
print(f"msg {message1} added. msg id:{message1_id}  ")

message2_id = r.xadd(redis_stream_name, message2)
print(f"msg {message2} added. msg id:{message2_id}  ")

# consume message from Redis stream
msg = r.xreadgroup(redis_stream_group_name,"consumer1",{redis_stream_name:">"},1,0)
print(msg)

r.xack(redis_stream_name,redis_stream_group_name,msg[0][1][0][0])

print("END.....")

