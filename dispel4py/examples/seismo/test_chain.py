from dispel4py.core import GenericPE, NAME, TYPE
from obspy.core import read


def PEMeanSub(pe, stream):
    pe.log(f"Running constant detrend on {stream}")
    return stream.detrend(type="constant")


def PEDetrend(pe, stream):
    pe.log(f"Running linear detrend on {stream}")
    return stream.detrend(type="linear")


class TestProducer(GenericPE):
    """
    This PE reads a file
    """

    def __init__(self):
        GenericPE.__init__(self)
        self.outputconnections = {
            "output": {NAME: "output", TYPE: ["timestamp", "location", "stream"]}
        }

    def process(self, inputs):
        stream = read(
            "/Users/akrause/VERCE/data/laquila/20100501-20120930_fseed/TERO/20100501.fseed"
        )
        return {"output": [{}, {}, {"data": stream}]}


from dispel4py.workflow_graph import WorkflowGraph

controlParameters = {"runId": "12345", "username": "amyrosa", "outputdest": "./"}

from dispel4py.seismo.obspy_stream import (
    createProcessingComposite,
    INPUT_NAME,
    OUTPUT_NAME,
)

chain = []
chain.append(PEMeanSub)
chain.append(PEDetrend)
composite = createProcessingComposite(chain, controlParameters=controlParameters)

producer = TestProducer()
graph = WorkflowGraph()
graph.connect(producer, "output", composite, INPUT_NAME)
