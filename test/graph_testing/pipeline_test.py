'''
This is a dispel4py graph which produces a pipleline workflow with one producer node (prod) and 5 consumer nodes.

.. image:: /api/images/pipeline_test.png

Execution:

* MPI: Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> dispel4py mpi <-f file containing the input dataset in JSON format>
	<-i number of iterations/runs'> <-s>

    When <-i number of interations/runs> is not indicated, the graph is executed once by default.

    For example::

        mpiexec -n 6 dispel4py mpi test.graph_testing.pipeline_test

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 6 nodes. For this reason we need at least 6 MPI processes to execute it.

    Output::

        Processes: {'TestProducer0': [1], 'TestOneInOneOut5': [5], 'TestOneInOneOut4': [4], 'TestOneInOneOut3': [3], 'TestOneInOneOut2': [2], 'TestOneInOneOut1': [0]}
        Rank 1: Sending terminate message to [0]
        TestProducer0 (rank 1): Processed 1 input block(s)
        TestProducer0 (rank 1): Completed.
        TestOneInOneOut3 (rank 3): I'm a bolt
        TestOneInOneOut5 (rank 5): I'm a bolt
        Rank 0: Sending terminate message to [2]
        TestOneInOneOut1 (rank 0): Processed 1 input block(s)
        TestOneInOneOut1 (rank 0): Completed.
        Rank 2: Sending terminate message to [3]
        TestOneInOneOut2 (rank 2): Processed 1 input block(s)
        TestOneInOneOut2 (rank 2): Completed.
        Rank 3: Sending terminate message to [4]
        TestOneInOneOut3 (rank 3): Processed 1 input block(s)
        TestOneInOneOut3 (rank 3): Completed.
        Rank 4: Sending terminate message to [5]
        TestOneInOneOut4 (rank 4): Processed 1 input block(s)
        TestOneInOneOut4 (rank 4): Completed.
        TestOneInOneOut5 (rank 5): Processed 1 input block(s)
        TestOneInOneOut5 (rank 5): Completed.

'''

from test.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph

def testPipeline(graph):
    '''
    Adds a pipeline to the given graph.

    :rtype: the created graph
    '''
    prod = t.TestProducer()
    prev = prod
    for i in range(5):
        cons = t.TestOneInOneOut()
        graph.connect(prev, 'output', cons, 'input')
        prev = cons
    return graph
''' important: this is the graph_variable '''
graph = testPipeline(WorkflowGraph())
