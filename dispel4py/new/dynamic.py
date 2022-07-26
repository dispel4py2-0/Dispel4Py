# import libraries
import argparse
import copy
import multiprocessing
import time
from multiprocessing import Process, Queue, TimeoutError
from dispel4py.new import processor
from queue import Empty


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        prog='dispel4py',
        description='Submit a dispel4py graph to zeromq multi processing')
    parser.add_argument('-ct', '--consumer-timeout',
                        help='stop consumers after timeout in ms',
                        type=int)
    parser.add_argument('-n', '--num', metavar='num_processes', required=True,
                        type=int, help='number of processes to run')
    
    result = parser.parse_args(args, namespace)
    return result


# This function is to get the destinations of a certain node in the graph
def _get_destination(graph, node, output_name):
    
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


# This class is written for PE when using PE.write() function
class GenericWriter():
    
    def __init__(self, q, node, output_name, workflow):
        self.q = q
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
                self.q.put((dest_id, {input_name: data}))


# This function is to process the data of the queue in the certain PE
def _communicate(pes, nodes, value, proc, q, workflow):

    try:
        pe_id, data = value
        # print('%s receive input: %s in process %s' % (pe_id, data, proc))
        
        pe = pes[pe_id]
        node = nodes[pe_id]
        
        for o in pe.outputconnections:
            pe.outputconnections[o]['writer'] = GenericWriter(q, node, o, workflow)
        
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
                        q.put((dest_id, {input_name: output_value}))

    except Exception as e:
        pass


# This function is to process the workflow in a certain process
def _process_worker(workflow, q, proc):
    
    pes = {node.getContainedObject().id: node.getContainedObject() for node in workflow.graph.nodes()}
    nodes = {node.getContainedObject().id: node for node in workflow.graph.nodes()}
    
    cnt = 1

    while True:

        if cnt == 1:
            value = q.get(True)
            _communicate(pes, nodes, value, proc, q, workflow)

        else:

            if not q.empty():

                try:
                    value = q.get(False)
                    _communicate(pes, nodes, value, proc, q, workflow)

                except Empty:
                    print('EMPTY!!!!!')
                    continue

            else:
                break

        cnt += 1


# This function is to process the workflow with given inputs and args
def process(workflow, inputs, args):
    
    elapsed_time=0
    start_time = time.time()
    
    manager = multiprocessing.Manager()
    q = manager.Queue()
    
    size = args.num
    
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
                    q.put((pe.id, {}))
 
            else:
                for d in provided_inputs:
                    q.put((pe.id, d))

    jobs = []
    for proc, workflow in workers.items():
        p = multiprocessing.Process(target=_process_worker, args=(workflow, q, proc))
        jobs.append(p)
    
    print('Starting %s workers communicating' % (len(workers)))
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()
    
    print ("ELAPSED TIME: "+str(time.time()-start_time))
