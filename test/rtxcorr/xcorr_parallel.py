from dispel4py.workflow_graph import WorkflowGraph 
from dispel4py.provenance import *
from dispel4py.new.processor  import *
import time
import random
import numpy
import traceback 
from dispel4py.base import create_iterative_chain, GenericPE, ConsumerPE, IterativePE, SimpleFunctionPE
from dispel4py.new.simple_process import process_and_return

import IPython
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats.stats import pearsonr 
import networkx as nx


sns.set(style="white")


class Start(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('iterations')
        self._add_output('output')
        #self.prov_cluster="myne"
    
    def _process(self,inputs):
        
        if 'iterations' in inputs:
            inp=inputs['iterations']
             
            self.write('output',inp,metadata={'iterations':inp})
            
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'

class Source(GenericPE):

    def __init__(self,sr,index,batchsize):
        GenericPE.__init__(self)
        self._add_input('iterations')
        self._add_output('output')
        self.sr=sr
        self.var_index=index
        self.batchsize=batchsize
        #self.prov_cluster="myne"
         
        self.parameters={'sampling_rate':sr}
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'
        
    
    def _process(self,inputs):
         
        if 'iterations' in inputs:
            iteration=inputs['iterations'][0]
        
        
        batch=[]
        #Streams out values at 1/self.sr sampling rate, until iteration>0
        while (iteration>0):
            while (len(batch)<self.batchsize):
                val=random.uniform(0,100)
                time.sleep(1/self.sr)
                batch.append(val)
                
            self.write('output',(iteration,self.var_index,batch),metadata={'var_index':self.var_index,'iteration':iteration})
            batch=[]
            iteration-=1
        

class MaxClique(GenericPE):

    def __init__(self,threshold):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('graph')
        self._add_output('clique')
        self.threshold=threshold
        #self.prov_cluster="myne"
         
        self.parameters={'threshold':threshold}
        
                
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'
        
    
    def _process(self,inputs):
         
        if 'input' in inputs:
            matrix=inputs['input'][0]
            batch=inputs['input'][1]
        
        
        self.log(matrix)
        self.write('graph',matrix,metadata={'matrix':str(matrix),'batch':batch})
        
        G = nx.from_numpy_matrix(matrix)
        labels = {i : i for i in G.nodes()}
        plt.figure(batch)
        pos = nx.circular_layout(G)
        nx.draw_circular(G)
        nx.draw_networkx_labels(G, pos, labels, font_size=15)
        #nx.draw(G)
        #fig1 = plt.gcf()
       # plt.close(fig1)
        
        low_values_indices = matrix < self.threshold  # Where values are low
        matrix[low_values_indices] = 0 
        self.log(matrix)
        self.write('clique',matrix,metadata={'matrix':str(matrix),'batch':batch})
                
         
        
        H = nx.from_numpy_matrix(matrix)
        plt.figure(2)
        labels = {i : i for i in H.nodes()}
        pos = nx.circular_layout(H)
        nx.draw_circular(H)
        nx.draw_networkx_labels(H, pos, labels, font_size=15)
       
         
       # plt.close()
         
       
        #Streams out values at 1/self.sr sampling rate, until iteration>0
        
        

class CompMatrix(GenericPE):

    def __init__(self,variables_number):
        GenericPE.__init__(self)
        self._add_input('input',grouping=[0]) 
        self._add_output('output')
        self.size=variables_number
        self.parameters={'variables_number':variables_number}
        self.data={}
         
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'self.prov_cluster='mycluster'
            
    def _process(self,inputs):
        for x in inputs:
            
            if inputs[x][0] not in self.data:
                #prepares the data to visualise the xcor matrix of a specific batch number.
                self.data[inputs[x][0]]={}
                self.data[inputs[x][0]]['matrix']=numpy.identity(self.size)
                self.data[inputs[x][0]]['ro_count']=0
            
            self.data[inputs[x][0]]['matrix'][inputs[x][1][1],inputs[x][1][0]]=inputs[x][2]
            self.data[inputs[x][0]]['ro_count']+=1
            self.log((inputs[x][0],self.data[inputs[x][0]]['ro_count']))
            
            if self.data[inputs[x][0]]['ro_count']==(self.size*(self.size-1))/2:
                matrix=self.data[inputs[x][0]]['matrix']
                
                d = pd.DataFrame(data=matrix,
                 columns=range(0,self.size),index=range(0,self.size))
                
                mask = numpy.zeros_like(d, dtype=numpy.bool)
                mask[numpy.triu_indices_from(mask)] = True

                # Set up the matplotlib figure
                f, ax = plt.subplots(figsize=(11, 9))

                # Generate a custom diverging colormap
                cmap = sns.diverging_palette(220, 10, as_cmap=True)

                # Draw the heatmap with the mask and correct aspect ratio
                sns.heatmap(d, mask=mask, cmap=cmap, vmax=1,
                    square=True,
                    linewidths=.5, cbar_kws={"shrink": .5}, ax=ax)
                
                sns.plt.show()   
                self.log(matrix)
                self.write('output',(matrix,inputs[x][0]),metadata={'matrix':str(d),'batch':str(inputs[x][0])})


            
class CorrCoef(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        #self._add_input('input1',grouping=[0])
        #self._add_input('input2',grouping=[0])
        self._add_output('output')
        self.data={}
        
        
         
        
    def _process(self, inputs):
        index=None
        val=None
              
        for x in inputs:
            if inputs[x][0] not in self.data:
                self.data[inputs[x][0]]=[]
                
            for y in self.data[inputs[x][0]]:
                #self.log([y,self.data])
                ro=numpy.corrcoef(y[1],inputs[x][2])
                self.log(((inputs[x][0],(inputs[x][1],y[0]),ro[0][1])))
                self.write('output',(inputs[x][0],(inputs[x][1],y[0]),ro[0][1]))
            
            if inputs[x][0] not in self.data:
                self.data[inputs[x][0]]=[]
            
            #appends var_index and value
            self.data[inputs[x][0]].append((inputs[x][1],inputs[x][2]))
     
 

 

     
####################################################################################################

#Declare workflow inputs: (each iteration prduces a batch_size of samples at the specified sampling_rate)
# number of projections = iterations/batch_size at speed defined by sampling rate
variables_number=7
sampling_rate=100
batch_size=5
iterations=5

input_data = {"Start": [{"iterations": [iterations]}]}
      
# Instantiates the Workflow Components  
# and generates the graph based on parameters

def createWf():
    graph = WorkflowGraph()
    mat=CompMatrix(variables_number)
    mat.prov_cluster='record2'
    mc = MaxClique(-0.01)
    mc.prov_cluster='record0'
    start=Start()
    start.prov_cluster='record0'
    sources={}
     

    
    
    cc=CorrCoef()
    cc.prov_cluster='record1'
    
      
    for i in range(0,variables_number):
        sources[i] = Source(sampling_rate,i,batch_size)
        sources[i].prov_cluster='record0'
        #'+str(i%variables_number)
        #+str(i%7)
        sources[i].numprocesses=1
        #sources[i].name="Source"+str(i)

    for h in range(0,variables_number):
        cc._add_input('input'+'_'+str(h+1),grouping=[0])
        graph.connect(start,'output',sources[h],'iterations')
        graph.connect(sources[h],'output',cc,'input'+'_'+str(h+1))
        
    
    graph.connect(cc,'output',mat,'input')
    graph.connect(mat,'output',mc,'input')
    
        
  
    return graph
        


print ("Preparing for: "+str(iterations/batch_size)+" projections" )


#Store via recorders or sensors
#ProvenanceRecorder.REPOS_URL='http://127.0.0.1:8080/j2ep-1.0/prov/workflow/insert'

#Store via service
ProvenancePE.REPOS_URL='http://127.0.0.1:8082/workflow/insert'

#Export data lineage via service (REST GET Call on dataid resource)
ProvenancePE.PROV_EXPORT_URL='http://127.0.0.1:8082/workflow/export/data/'

#Store to local path
ProvenancePE.PROV_PATH='./prov-files/'

#Size of the provenance bulk before sent to storage or sensor
ProvenancePE.BULK_SIZE=1

#ProvenancePE.REPOS_URL='http://climate4impact.eu/prov/workflow/insert'
 

def createGraphWithProv():
    
    graph=createWf()
    #Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder subtypes

    # Ranomdly generated unique identifier for the current run
    rid='CORR_SIMPLE_'+getUniqueId()

    
    # Finally, provenance enhanced graph is prepared:


    ##Initialise provenance storage in files:
    #profile_prov_run(graph,None,provImpClass=(ProvenancePE,),componentsType={'CorrCoef':(ProvenancePE,)},username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='file')
                  # skip_rules={'CorrCoef':{'ro':{'$lt':0}}})

    #Initialise provenance storage to service:
    #profile_prov_run(graph,None,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='service')
                   #skip_rules={'CorrCoef':{'ro':{'$lt':0}}})

    #clustersRecorders={'record0':ProvenanceRecorderToFileBulk,'record1':ProvenanceRecorderToFileBulk,'record2':ProvenanceRecorderToFileBulk,'record6':ProvenanceRecorderToFileBulk,'record3':ProvenanceRecorderToFileBulk,'record4':ProvenanceRecorderToFileBulk,'record5':ProvenanceRecorderToFileBulk}
    #Initialise provenance storage to sensors and Files:
    #profile_prov_run(graph,ProvenanceRecorderToFile,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
    #clustersRecorders=clustersRecorders)
    
    #Initialise provenance storage to sensors and service:
    #profile_prov_run(graph,ProvenanceRecorderToService,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
   
    #Summary view on each component
    #profile_prov_run(graph,ProvenanceTimedSensorToService,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
   
   
   
    #Configuring provenance feedback-loop
    #profile_prov_run(graph,ProvenanceTimedSensorToService,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor',feedbackPEs=['Source','MaxClique'])
   
   
    #Initialise provenance storage end associate a Provenance type with specific components:
    profile_prov_run(graph,None, provImpClass=(ProvenancePE,),
                     username='aspinuso',
                     runId=rid,
                     w3c_prov=False,
                     description="provState",
                     workflowName="test_rdwd",
                     workflowId="xx",
                     componentsType= {'MaxClique':{'type':(SingleInvocationStateDep,),'state_dep_port':'graph'},
                     save_mode='service')

    #
    return graph

#graph=createWf()
graph=createGraphWithProv()

from dispel4py.visualisation import display
display(graph)
