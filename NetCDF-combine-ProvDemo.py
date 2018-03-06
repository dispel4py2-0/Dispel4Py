
# coding: utf-8

# #  Provenance integration in netcdf/xarray Data-Intensive workflows
# 
# 
# #### Authors: Alessandro Spinuso and Andrej Mihajlovski
# 
# ####  Royal Netherlands Meteorological Institure (KNMI)
# 
# 
# The following "Live" notebook demonstrates a simple workflow implemented with a data-intensive processing library (dispel4py), that has been extended with a configurable and programmable provenance tracking framework (Active provenance).
# 
# ## Management Highligts - Active Provenance, S-PROV and S-ProvFlow
# <ul>
# <li>
# The provenance information produced can be tuned and adapted to computational, precision and contextualisation requirements</li>
# <li>
# The Active freamework allows for the traceability of data-reuse across different executions, methods and users</li>
# <li>The provenance can be stored as files or sent at run-time to an external repository (S-ProvFlow)</li>
# <li>The repository can be searched and explored via interactive tools</li> 
# <li>The provenance model is designed around an hybrid data-flow model, which takes into account data-streams and concrente data resourcese. eg. file location, webservices etc.</li>
# <li>The lineage can be exported from the repository in W3C PROV format. This facilitates the production of interoperabile reports and data-curation tasks. For instance, The provenance related to specific data can be stored in W3C-PROV XML format into strucutred file formats (NetCDF) as well as istitutional and general-purpose citable data-repositories.</li>
# </ul>
# 
# ## Demonstration outline
# 
# ### 1 - Workflow specification and execution
# 
# <ol>
#   <li>Define the <i><b>Classes</b></i> of the <i><b>Workflow Components</b></i></li>
#   <li>Construct the <i><b>Workflow</b></i> application</li>
#   <li>Prepare the Input</li>
#   <li>Visualise and run the workflow without provenance</li>
# </ol>
# 
# ### 2 - Provenance Types, Profiling and contextualisation
# 
# <ol>
#   <li>Define the <i><b>Provenance Type</b></i> to be used within the workflow</li>
#   <li><i><b>Profile</b></i> the workfow for provenance tracking</li>
#   <li>Visualise and run workfow with provenance activatied</li>
#   <li>Export and embed provenance within NetCDF results</li>
#   <li>Explore the resulting provenance with interactive and static visualsations</li>
# </ol>
# 
# ### 3 - Data-reuse traceability. 
# <ol>
#   <li>Change the input and demostrate consistency of provenance for data-ruse across multiple workflow executions</li>
#   <li>Discuss more complex use cases and configuration options</li>
# </ol>
# 
# ### 4 - Informal Evaluation
# 
# SWOT form:
# 
# https://docs.google.com/presentation/d/10xlRYytR7NB9iC19T29BD-rW77ZAtnjtlukMJDP_MIs/edit?usp=sharing
# 
# 
# ## 1 - Workflow specification and execution
# 
# 
# <ul>
# <li>The dispel4py framework is utilised for the workflows</li>
# <li>Xarray for inmemory management of netcdf/opendap data.</li>
# <li>Matplotlib for visualisation.</li>
# <li>W3C for provenance representation.</li>
# </ul>

# In[1]:

import xarray
#import netCDF4
import json

from dispel4py.workflow_graph import WorkflowGraph 
from dispel4py.provenance import *

from collections import OrderedDict
import time
import random

from dispel4py.base import create_iterative_chain, ConsumerPE, IterativePE, SimpleFunctionPE

import matplotlib.pyplot as plt
import traceback

from pprint import pprint


# Simple Workflow, xarray in xarray out. 
# The generic processing elements are defined below. the <i>GenericPE</i> bellongs to the dispel4py framework. It allows data-objects to be passed as inputs and outputs. The <i>Components</i> are linked and visualised via the workflow_graph module.
# 
# ### 1.1 The Four Workflow Components:
# 
# <ol>
# <li>- Read, xarray is read into memory.</li>
# <li>- ANALYSIS, xarray is processed/passed to output (dummy, no real changes in the example)</li>
# <li>- Write, xarray is visualised.</li>
# <li>- Combine, two xarray are combined into one ds.</li>
# </ol>

# In[2]:

class Read(GenericPE):
    
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('xarray')
        self.count=0

    def _process(self,inputs):
        self.log('Read_Process')
     
        self.log(inputs)
        
        inputLocation = inputs['input'][0]

        ds = xarray.open_dataset( inputLocation )
        
        self.write( 'xarray' , (ds, self.count) , location=inputLocation )
        self.count+=1
            
class Write(IterativePE):
    
    def __init__(self):
        IterativePE.__init__(self)
        self._add_input('input')
        self._add_output('location')
         
        
    def _process(self,inputs):
        self.log('Write_Function')
        outputLocation = "data/new_"+str(inputs[0].attrs['id'])+".nc"
        inputs[0].to_netcdf( outputLocation )
        self.log(outputLocation)
        self.write('location', outputLocation,location=outputLocation )
        
        
class Analysis(GenericPE):
        
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
         
        
    def _process(self,inputs):
        self.log('Workflow_process')
        #self.log( len(inputs))
        
        #nc = inputs['input'][0]
        
        nc = inputs['input'][0]
        #self.log(nc)
        #
        self.write('output', (nc,inputs['input'][1]),metadata={'index':inputs['input'][1]})
         
        


class Combine(GenericPE):
     
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('combine1',grouping=[1])
        self._add_input('combine2',grouping=[1])
        self._add_output('combo')
        self.count=0
        self.data1=[]
        self.data2=[]
        self.nc1 = None
        self.nc2 = None
        self.out = None
        
    def _process(self,inputs):
        self.log('Combine_process')
        self.log(inputs.keys())
        
        if 'combine1' in inputs.keys():
            self.data1.append(inputs['combine1'][0])
            
        
        if 'combine2' in inputs.keys():
            self.data2.append(inputs['combine2'][0])
            
        
        
        if (len(self.data1)>0 and len(self.data2)>0):
            #self.log("LEN"+str(len(self.data1)))
            nc1 =  self.data1.pop(0)
            nc2 =  self.data2.pop(0)
            
            nc=nc1
            # numpy arithmetic for DataArrays...
             
            #self.log(nc2)
            for k,v in nc2.attrs.items():
                if k in nc.attrs.keys():
                    nc.attrs[k] = v
                else:
                    nc.attrs[k] = v
            
           
            
            self.write('combo', (nc,self.count))
            self.count+=1





# ### 1.2 Construct the Workflow application
# 
# Instantiates the Components and combines them in a workflow graph which gets eventually visualised.
# 
# Siz Instances are created from the above PEs.

# In[3]:


def createWorkflowGraph():
    readX  = Read()
    readX.name = 'COLLECTOR1'
    readY  = Read()
    readY.name = 'COLLECTOR2'
    
    analyse   = Analysis()
    analyse.name    = 'ANALYSIS'
    analyse.parameters = { 'filter': 10 }

    analyse2   = Analysis()
    analyse2.name    = 'ANALYSIS'
    analyse2.parameters = { 'filter': 13 }
    
    wf3     = Combine()
    wf3.name    = 'COMBINE'
    wf3.parameters = { 'wf':'paramC' }
    
    writeX = Write()
    writeX.name = 'STORE'
    
    
    graph = WorkflowGraph()    
    
    graph.connect(readX ,'xarray'   , analyse      ,'input')
    graph.connect(readY ,'xarray'   , analyse2     ,'input')
    
    graph.connect( analyse  ,'output'   , wf3     ,'combine1')
    graph.connect( analyse2 ,'output'   , wf3     ,'combine2')
    
    graph.connect(wf3    ,'combo'   , writeX , 'input')

    return graph



graph = createWorkflowGraph()

 








# ### 1.3 Run the Workflow
# 
# 

# In[5]:

global result

def runExampleWorkflow():
                                                     
    print input_data                   

    #Launch in simple process
    result = simple_process.process_and_return(graph, input_data)
    print "\n RESULT: "+str(result)


# In[6]:

#runExampleWorkflow()


# ## 2 - Provenance Types, Profiling and contextualisation
# 
# ### 2.1 Define a Provenance Type
# 
# Once the Provenance types have been defined, these are used to configure, or profile, a workflow execution to comply with the desired provenance collection requirements.  Below we illustrate the framework method and the details of this approach.
# 
# <ul>
#
# <li><b><i>profile_prov_run</i></b> With this method, the users of the workflow can profile their run for provenance by indicating which types to apply to each component. Users can also chose where to store the metadata, locally to the file system or to a remote service. These operations can be performed in bulks, with different impacts on the overall overhead and on the experienced rapidity of the access of the lineage information. Finally, also general information about the attribution of the run, such as <i>username, run_id, description, workflow_name, workflow_id</i> are captured and included within the provenance traces.
# </li>
# <li><b><i>applyFlowResetPolicy (Advanced)</i></b>
# This method is invoked by each iteration when a decision has to be made on the required lineage pattern. The framework automatically passes information whether the invocation has produced any output or not (<i>on-void-iterations</i>). The method, according to predefined rules, provides indications on either discarding the current input data or to include it into the <i>StateCollection</i> automatically, capturing its contribution to the next invocation through a <i>stateful</i>operations. 
# In our implementation, basic provenance types such as <i>StatefulType</i> and <i>StatelessType</i> are made available and can be used accordingly the specific needs.
# </li>
# 
# <li><b><i>Skip-Rules (Advanced)</i></b>
# Users can tune the scale of the records produced by indicating in the above method a set of <i>skip-rules</i> for every component.This functionality allows users to specify rules to control the data-driven production of the provenance declaratively. The approach takes advantage of the contextualisation applied by the provenance types, which extract domain and experimental metadata, and evaluates their value against simple <i>skip-rule</i> of this kind:
# </li>
# </ul>
# 

# In[7]:

class netcdfProvType(ProvenanceType):
    def __init__(self):
        ProvenanceType.__init__(self)
        self.addNamespacePrefix("clipc","http://clipc.eu/ns/#")
    
    def extractExternalInputDataId(self,data, input_port):
        #Extract here the id from the data (type specific):

        self.log('ANDREJ.extractExternalInputDataId')
        #self.log(data)
        
        try:
            #ds = xarray.open_dataset(data['input'][0])
            ds = xarray.open_dataset(data[0])
            id = ds.attrs['id']
            
        except Exception, err:
            id = str(uuid.uuid1())
            #self.log(str(err))
        #Return
        return id
     
    
    def makeUniqueId(self, data, output_port):      
        
        self.log('ANDREJ.makeUniqueId')
        #self.log(kwargs)
        
        #produce the id
        id=str(uuid.uuid1())
            
        ''' if nc data '''
        if data!=None:
            xa = data[0]
        
            ''' unique as defined by the community standard '''
            xa.attrs['id'] = id
        
        #Return
        return id 
    

    
    ''' extracts xarray metadata '''
    def extractItemMetadata(self, data, output_port):
         
        self.log('ANDREJ.extractItemMetadata')
        #self.log(data)
        
        try:            
            nc_meta = OrderedDict()
            
            ''' cycle throug all attributes, dimensions and variables '''
            xa = data[0]
            #self.log(data)
            # dataset meta
            nc_meta['Dimensions'] = str( dict(xa.dims)) 
            nc_meta['Type'] = str(type(xa))
            
            # global attr
            #for k , v in xa.attrs.items():
            #    nc_meta['clipc:'+str(k).replace(".","_")] = str(v)
            # vars attr   
            for n , i in xa.data_vars.items():
                for k , v in i.attrs.items():
                    nc_meta['clipc:'+n+"_"+str(k).replace(".","_")] = str(v)[0:25]
            
            #pprint(nc_meta)
        
            metadata = [nc_meta]
            
            return metadata
                             
        except Exception, err:
            self.log("Applying default metadata extraction"+str(traceback.format_exc()))
            self.error=self.error+"Applying default metadata extraction:"+str(traceback.format_exc())
            return super(netcdfProvType, self).extractItemMetadata(data,output_port);
        
        



# 
# ### 2.2 Profile the workfow for provenance tracking
# 
# Once the Provenance types have been defined, these are used to configure, or profile, a workflow execution to comply with the desired provenance collection requirements.  Below we illustrate the framework method and the details of this approach.
# 
# <ul>
# 
# <li><b><i>profile_prov_run</i></b> With this method, the users of the workflow can profile their run for provenance by indicating which types to apply to each component. Users can also chose where to store the metadata, locally to the file system or to a remote service. These operations can be performed in bulks, with different impacts on the overall overhead and on the experienced rapidity of the access of the lineage information. Additional details on the proposed remote provenance storage and access service will be provided in Chapter V. Finally, also general information about the attribution of the run, such as <i>username, run_id, description, workflow_name, workflow_id</i> are captured and included within the provenance traces.
# </li>
# <li><b><i>Sel-Rules (Advanced)</i></b>
# Users can tune the scale of the records produced by indicating in the above method a set of <i>sel-rules</i> for every component.This functionality allows users to specify rules to control the data-driven production of the provenance declaratively. The approach takes advantage of the contextualisation applied by the provenance types, which extract domain and experimental metadata, and evaluates their value against simple <i>sel-rule</i> of this kind:
# </li>
# </ul>
# 

# In[8]:

sel_rules={"ANALYSIS":{"term":{"$gt":0,"$lt":100}}}


# A high level 'template/profile' describing the provenance process.

# In[9]:

prov_config =  {
                    'provone:User': "aspinuso", 
                    's-prov:description' : "provdemo combo double",
                    's-prov:workflowName': "demo_ecmwf",
                    's-prov:workflowType': "clipc:combine",
                    's-prov:workflowId'  : "workflow process",
                    's-prov:save-mode'   : 'service'         ,
                    # defines the Provenance Types and Provenance Clusters for the Workflow Components
                    's-prov:componentsType' : 
                                       {'ANALYSIS': {'s-prov:type':(netcdfProvType,),
                                                     's-prov:prov-cluster':'clipc:Combiner'},
                                        'COMBINE':  {'s-prov:type':(netcdfProvType, Nby1Flow,),
                                                     's-prov:prov-cluster':'clipc:Combiner'},
                                        'COLLECTOR1':{'s-prov:prov-cluster':'clipc:DataHandler'},
                                        'COLLECTOR2':{'s-prov:prov-cluster':'clipc:DataHandler'},
                                        'STORE':    {'s-prov:prov-cluster':'clipc:DataHandler'}
                                        },
                    's-prov:sel-rules': None
                } 


# The REPOS_URL is the target provenence depo. Used as a production tool for VERCE (Seismo), CLIPC (C3S) and Climate4Impact (Climate IS-ENES)

# In[10]:

#Store via service
ProvenanceType.REPOS_URL='http://127.0.0.1:8082/workflowexecutions/insert'
#ProvenanceType.REPOS_URL='http://climate4impact.eu/prov/workflow/insert'

#Export data lineage via service (REST GET Call on dataid resource)
#ProvenanceType.PROV_EXPORT_URL='http://127.0.0.1:8082/data/'
ProvenanceType.PROV_EXPORT_URL='http://127.0.0.1:8082/data/'
#ProvenanceType.PROV_EXPORT_URL="http://climate4impact.eu/prov/workflow/export/data/" 


#Store to local path
ProvenanceType.PROV_PATH='./prov-files/'

#Size of the provenance bulk before sent to storage or sensor
ProvenanceType.BULK_SIZE=2

#ProvenanceType.REPOS_URL='http://climate4impact.eu/prov/workflow/insert'


# In[11]:

def createGraphWithProv():
    
    graph=createWorkflowGraph()
    #Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder subtypes

    # Ranomdly generated unique identifier for the current run
    rid='JUP_COMBINE_'+getUniqueId()

    
    # Finally, provenance enhanced graph is prepared:
    print prov_config

     
    #Initialise provenance storage to service:
    configure_prov_run(graph, 
                     provImpClass=(ProvenanceType,),
                     username=prov_config['provone:User'],
                     runId=rid,
                     description=prov_config['s-prov:description'],
                     workflowName=prov_config['s-prov:workflowName'],
                     workflowType=prov_config['s-prov:workflowType'],
                     workflowId=prov_config['s-prov:workflowId'],
                     save_mode=prov_config['s-prov:save-mode'],
                     componentsType=prov_config['s-prov:componentsType']
                      
                    )
                   

    #clustersRecorders={'record0':ProvenanceRecorderToFileBulk,'record1':ProvenanceRecorderToFileBulk,'record2':ProvenanceRecorderToFileBulk,'record6':ProvenanceRecorderToFileBulk,'record3':ProvenanceRecorderToFileBulk,'record4':ProvenanceRecorderToFileBulk,'record5':ProvenanceRecorderToFileBulk}
    #Initialise provenance storage to sensors and Files:
    #profile_prov_run(graph,ProvenanceRecorderToFile,provImpClass=(ProvenanceType,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
    #clustersRecorders=clustersRecorders)
    
    #Initialise provenance storage to sensors and service:
    #profile_prov_run(graph,ProvenanceRecorderToService,provImpClass=(ProvenanceType,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
   
    #Summary view on each component
    #profile_prov_run(graph,ProvenanceTimedSensorToService,provImpClass=(ProvenanceType,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
   
   
   
    #Configuring provenance feedback-loop
    #profile_prov_run(graph,ProvenanceTimedSensorToService,provImpClass=(ProvenanceType,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor',feedbackPEs=['Source','MaxClique'])
   
   
    #Initialise provenance storage end associate a Provenance type with specific components:
    #profile_prov_run(graph,provImpClass=ProvenanceType,componentsType={'Source':(ProvenanceStock,)},username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='service')

    #
    return graph


graph=createGraphWithProv()
#graph = createWorkflowGraph()

#display(graph)






