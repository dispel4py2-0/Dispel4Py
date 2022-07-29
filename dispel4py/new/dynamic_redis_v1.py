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
import argparse
import uuid
import redis
import copy
import multiprocessing
import time
import json
from dispel4py.new import processor

# ====================
# Constants
# ====================
# whether to enable process auto terminate when no more message received
AUTO_TERMINATE_ENABLE = True
# process will auto terminate after such idle time in seconds
PROCESS_AUTO_TERMINATE_MAX_IDLE = 10

# Redis stream prefix
REDIS_STREAM_PREFIX = "DISPEL4PY_DYNAMIC_STREAM_"
# Redis group prefix
REDIS_STREAM_GROUP_PREFIX = "DISPEL4PY_DYNAMIC_GROUP_"

# Redis stream data type must be a dict, this is the key
REDIS_STREAM_DATA_DICT_KEY = b'0'

# Redis message count pre-read
REDIS_READ_COUNT = 1

# Redis read parameter. To enable its blocking read and never timeout
REDIS_BLOCKING_FOREVER = 0
# Redis read parameter. To enable its blocking read with a suitable timeout
REDIS_STATELESS_STREAM_READ_TIMEOUT = 1000


def parse_args(args, namespace):
    """
        Parse args for dynamic redis
    """
    parser = argparse.ArgumentParser(prog='dispel4py',
                                     description='Submit a dispel4py graph to redis dynamic processing')
    parser.add_argument('-ri', '--redis-ip', required=True, help='IP address of external redis server')
    parser.add_argument('-rp', '--redis-port', help='External redis server port,default 6379', type=int, default=6379)
    # parser.add_argument('-ct', '--consumer-timeout', help='stop consumers after timeout in ms', type=int)
    parser.add_argument('-n', '--num', metavar='num_processes', required=True, type=int,
                        help='number of processes to run')
    result = parser.parse_args(args, namespace)
    return result


def _get_destination(graph, node, output_name):
    """
        This function is to get the destinations of a certain node in the graph
    """
    result = set()
    pe_id = node.getContainedObject().id

    for edge in graph.edges(node, data=True):

        direction = edge[2]['DIRECTION']
        source = direction[0]
        dest = direction[1]

        if source.id == pe_id and output_name == edge[2]['FROM_CONNECTION']:
            dest_input = edge[2]['TO_CONNECTION']
            result.add((dest.id, dest_input))

    return result


class RedisWriter():
    """
        This class is written for PE when using PE.write() function, write data to redis
    """

    def __init__(self, r, redis_stream_name, node, output_name, workflow):
        self.r = r
        self.redis_stream_name = redis_stream_name
        self.node = node
        self.output_name = output_name
        self.workflow = workflow

    def write(self, data):

        # get the destinations of the PE
        destinations = _get_destination(self.workflow.graph, self.node, self.output_name)

        # if the PE has no destinations, then print the data
        if not destinations:
            print('Output collected from %s: %s' % (self.node.getContainedObject().id, data))
        # otherwise, put the data in the destinations to the queue
        else:
            for dest_id, input_name in destinations:
                # print('sending to %s with value: %s' % (dest_id, data))
                self.r.xadd(self.redis_stream_name,
                       {REDIS_STREAM_DATA_DICT_KEY: json.dumps((dest_id, {input_name: data}))})


def _communicate(pes, nodes, value, proc, r, redis_stream_name, workflow):
    """
        This function is to process the data of the queue in the certain PE
    """
    try:
        pe_id, data = value
        # print('%s receive input: %s in process %s' % (pe_id, data, proc))

        pe = pes[pe_id]
        node = nodes[pe_id]

        # TODO should found the right target stream name. Some for stateful, some for stateless.
        for o in pe.outputconnections:
            pe.outputconnections[o]['writer'] = RedisWriter(r, redis_stream_name, node, o, workflow)

        output = pe.process(data)

        if output:
            for output_name, output_value in output.items():
                # get the destinations of the PE
                destinations = _get_destination(workflow.graph, node, output_name)
                # if the PE has no destinations, then print the data
                if not destinations:
                    print('Output collected from %s: %s in process %s' % (pe_id, output_value, proc))
                # otherwise, put the data in the destinations to the queue
                else:
                    for dest_id, input_name in destinations:
                        # print('sending to %s with value: %s in processs %s' % (dest_id, output_value, proc))
                        # q.put((dest_id, {input_name: output_value}))
                        r.xadd(redis_stream_name,
                               {REDIS_STREAM_DATA_DICT_KEY: json.dumps((dest_id, {input_name: output_value}))})

    except Exception as e:
        pass


def _process_worker(workflow, redis_ip, redis_port, redis_stream_name, redis_stream_group_name, proc):
    """
        This function is to process the workflow in a certain process
    """
    pes = {node.getContainedObject().id: node.getContainedObject() for node in workflow.graph.nodes()}
    nodes = {node.getContainedObject().id: node for node in workflow.graph.nodes()}

    # connect to redis
    r = redis.Redis(redis_ip, redis_port)

    cnt = 1
    # for auto terminate, save the last process data time
    last_process = time.time()
    while True:

        if cnt == 1:
            # block = 0 means blocking read
            response = r.xreadgroup(redis_stream_group_name, f"consumer:{proc}", {redis_stream_name: ">"},
                                    REDIS_READ_COUNT, REDIS_BLOCKING_FOREVER, True)

            redis_id, value = decode_redis_stream_data(response)
            _communicate(pes, nodes, value, proc, r, redis_stream_name, workflow)

        else:
            '''Queue.get() if optional args 'block' is true and 'timeout' is None (the default), block if necessary 
            until an item is available. If 'timeout' is a non-negative number, it blocks at most 'timeout' seconds and 
            raises the Empty exception if no item was available within that time. Otherwise ('block' is false), return 
            an item if one is immediately available, else raise the Empty exception ('timeout' is ignored in that case). 
            '''

            response = r.xreadgroup(redis_stream_group_name, f"consumer:{proc}", {redis_stream_name: ">"},
                                    REDIS_READ_COUNT, REDIS_STATELESS_STREAM_READ_TIMEOUT, True)

            if not response:
                # read timeout, because no data, continue to read
                print(f"consumer:{proc} get no data in {REDIS_STATELESS_STREAM_READ_TIMEOUT}ms.")
                if AUTO_TERMINATE_ENABLE & (time.time() - last_process > PROCESS_AUTO_TERMINATE_MAX_IDLE):
                    print(f"TERMINATED: process:{proc} ends now")
                    break
                continue
            else:
                redis_id, value = decode_redis_stream_data(response)

                _communicate(pes, nodes, value, proc, r, redis_stream_name, workflow)

                # update last process time
                last_process = time.time()

        cnt += 1


def decode_redis_stream_data(redis_response):
    """
        Decode the data of redis stream, return the redis id and value
    """
    key, message = redis_response[0]
    redis_id, data = message[0]
    value = json.loads(data.get(REDIS_STREAM_DATA_DICT_KEY))
    return redis_id, value


def process(workflow, inputs, args):
    """
        This function is to process the workflow with given inputs and args
    """
    elapsed_time = 0
    start_time = time.time()

    # create redis stream and group

    jobid = str(uuid.uuid1())
    # Redis stream name & group name
    redis_stream_name = REDIS_STREAM_PREFIX + jobid
    redis_stream_group_name = REDIS_STREAM_GROUP_PREFIX + jobid

    # connect to redis
    redis_connection = redis.Redis(args.redis_ip, args.redis_port)

    # redis stream delete existing
    if redis_connection.exists(redis_stream_name):
        redis_connection.delete(redis_stream_name)

    # create consumer group, read FIFO, auto create stream
    redis_connection.xgroup_create(redis_stream_name, redis_stream_group_name, "$", True)

    size = args.num

    # init workers
    workers = {}
    for proc in range(size):
        cp = copy.deepcopy(workflow)
        cp.rank = proc
        workers[proc] = cp

    for node in workflow.graph.nodes():
        pe = node.getContainedObject()
        provided_inputs = processor.get_inputs(pe, inputs)

        if provided_inputs is not None:
            if isinstance(provided_inputs, int):
                for i in range(provided_inputs):
                    # q.put((pe.id, {}))
                    # Cannot add an tuple because XADD fields must be a non-empty dict
                    # Cannot add a dict because Invalid input of type: 'dict'.
                    redis_connection.xadd(redis_stream_name, {REDIS_STREAM_DATA_DICT_KEY: json.dumps((pe.id, {}))})
            else:
                for d in provided_inputs:
                    # q.put((pe.id, d))
                    redis_connection.xadd(redis_stream_name, {REDIS_STREAM_DATA_DICT_KEY: json.dumps((pe.id, d))})

    # init jobs
    jobs = []
    for proc, workflow in workers.items():
        p = multiprocessing.Process(target=_process_worker, args=(
            workflow, args.redis_ip, args.redis_port, redis_stream_name, redis_stream_group_name, proc))
        jobs.append(p)

    print('Starting %s workers communicating' % (len(workers)))
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()

    if AUTO_TERMINATE_ENABLE:
        print(f"ELAPSED TIME(minus {PROCESS_AUTO_TERMINATE_MAX_IDLE} second(s) for AUTO_TERMINATE): " + str(
            time.time() - start_time - PROCESS_AUTO_TERMINATE_MAX_IDLE))
    else:
        print("ELAPSED TIME: " + str(time.time() - start_time))
