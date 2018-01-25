from dispel4py.workflow_graph import WorkflowGraph 
from dispel4py.provenance import *
from dispel4py.seismo.seismo import *
from dispel4py.new.processor  import *
import time
import random
import numpy
import traceback 
from dispel4py.base import create_iterative_chain, GenericPE, ConsumerPE, IterativePE, SimpleFunctionPE
from dispel4py.new.simple_process import process_and_return
from dispel4py.visualisation import display

import IPython
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats.stats import pearsonr 
import networkx as nx
from itertools import combinations

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
         
        self.parameters={'sampling_rate':sr,'batchsize':batchsize}
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'
        
    
    def _process(self,inputs):
         
        if 'iterations' in inputs:
            iteration=inputs['iterations'][0]
        
        
        batch=[]
        it=1
        #Streams out values at 1/self.sr sampling rate, until iteration>0
        while (it<=iteration):
            while (len(batch)<self.batchsize):
                val=random.uniform(0,100)
                time.sleep(1/self.sr)
                batch.append(val)
                
            self.write('output',(it,self.var_index,batch),metadata={'var':self.var_index,'iteration':it,'batch':batch})
            batch=[]
            it+=1
        

class MaxClique(GenericPE):

    def __init__(self,threshold):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('graph')
        self._add_output('cliques')
        self.threshold=threshold
        #self.prov_cluster="myne"
         
        self.parameters={'threshold':threshold}
        
                
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'
        
    
    def _process(self,inputs):
         
        if 'input' in inputs:
            matrix=inputs['input'][0]
            iteration=inputs['input'][1]
        
        
         
       
        
        low_values_indices = matrix < self.threshold  # Where values are low
        matrix[low_values_indices] = 0 
        #plt.figure('graph_'+str(iteration))
                
         
        
        H = nx.from_numpy_matrix(matrix)
        fig = plt.figure('graph_'+str(iteration))
        text = "Iteration "+str(iteration)+" "+"graph"
         
        labels = {i : i for i in H.nodes()}
        pos = nx.circular_layout(H)
        nx.draw_circular(H)
        nx.draw_networkx_labels(H, pos, labels, font_size=15)
        fig.text(.1,.1,text)
        self.write('graph',matrix,metadata={'graph':str(matrix),'batch':iteration})
       
         
        
       # labels = {i : i for i in H.nodes()}
       # pos = nx.circular_layout(H)
       # nx.draw_circular(H)
       # nx.draw_networkx_labels(H, pos, labels, font_size=15)
       
        
        cliques = list(nx.find_cliques(H))
        
        fign=0
        maxcnumber=0
        maxclist=[]
        
        for nodes in cliques:
            if (len(nodes)>maxcnumber):
                maxcnumber=len(nodes)
                
        for nodes in cliques:
            if (len(nodes)==maxcnumber):
                maxclist.append(nodes)

        for nodes in maxclist:    
            edges = combinations(nodes, 2)
            C = nx.Graph()
            C.add_nodes_from(nodes)
            C.add_edges_from(edges)
            fig = plt.figure('clique_'+str(iteration)+str(fign))
            text = "Iteration "+str(iteration)+" "+"clique "+str(fign)
            fign+=1
            labels = {i : i for i in C.nodes()}
            pos = nx.circular_layout(C)
            nx.draw_circular(C)
            nx.draw_networkx_labels(C, pos, labels, font_size=15)
            fig.text(.1,.1,text)
            self.write('cliques',cliques,metadata={'clique':str(nodes),'label':text, 'order':maxcnumber}, location="file://cliques/")
        
        
        
#        C = nx.make_max_clique_graph(H)
#        plt.figure('clique_'+str(iteration))
#        labels = {i : i for i in C.nodes()}
##        pos = nx.circular_layout(C)
#        nx.draw_circular(C)
#        nx.draw_networkx_labels(C, pos, labels, font_size=15)
         
       
        #Streams out values at 1/self.sr sampling rate, until iteration>0
        
        

class CorrMatrix(GenericPE):

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
            
            self.data[inputs[x][0]]['matrix'][inputs[x][1][0]-1,inputs[x][1][1]-1]=inputs[x][2]
            self.data[inputs[x][0]]['ro_count']+=1
            #self.log((inputs[x][0],self.data[inputs[x][0]]['ro_count']))
            ##self.update_prov_state('iter_'+str(inputs[x][0]),None,metadata={'iter_'+str(inputs[x][0]):inputs[x][1]},dep=['iter_'+str(inputs[x][0])])
            
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
                self.write('output',(matrix,inputs[x][0]),metadata={'matrix':str(d),'iteration':str(inputs[x][0])})
                ##dep=['iter_'+str(inputs[x][0])])


            
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
                ro=numpy.corrcoef(y[2],inputs[x][2])
                #self.log(((inputs[x][0],(y[0],inputs[x][1]),ro[0][1])))
                #self.write('output',(inputs[x][0],(y[1],inputs[x][1]),ro[0][1]),metadata={'iteration':inputs[x][0],'vars':str(y[1])+"_"+str(inputs[x][1]),'ro':ro[0][1]},dep=['var_'+str(y[1])+"_"+str(y[0])])
                self.write('output',(inputs[x][0],(y[1],inputs[x][1]),ro[0][1]),metadata={'iteration':inputs[x][0],'vars':str(y[1])+"_"+str(inputs[x][1]),'ro':ro[0][1]})
            
            
            
            #appends var_index and value
            self.data[inputs[x][0]].append(inputs[x])
            #self.log(self.data[inputs[x][0]])
            #self.update_prov_state('var_'+str(inputs[x][1])+"_"+str(inputs[x][0]),inputs[x],metadata={'var_'+str(inputs[x][1]):inputs[x][2]}, ignore_inputs=False)
            
     

     
####################################################################################################

#Declare workflow inputs: (each iteration prduces a batch_size of samples at the specified sampling_rate)
# number of projections = iterations/batch_size at speed defined by sampling rate
variables_number=10
sampling_rate=100
batch_size=5
iterations=20

input_data = {"Start": [{"iterations": [iterations]}]}
      
# Instantiates the Workflow Components  
# and generates the graph based on parameters

variables_number=10
sampling_rate=100
batch_size=3
iterations=2

input_data = {"Start": [{"iterations": [iterations]}]}
      
# Instantiates the Workflow Components  
# and generates the graph based on parameters

def createWf():
    graph = WorkflowGraph()
    mat=CorrMatrix(variables_number)
    mat.prov_cluster='record2'
    mc = MaxClique(-0.01)
    mc.prov_cluster='record0'
    start=Start()
    start.prov_cluster='record0'
    sources={}
     

    
    
    cc=CorrCoef()
    cc.prov_cluster='record1'
    stock=['NASDAQ','MIBTEL','DOWJ']
      
    for i in range(1,variables_number+1):
        sources[i] = Source(sampling_rate,i,batch_size)
        sources[i].prov_cluster=stock[i%len(stock)-1]
        sources[i].numprocesses=1
        #sources[i].name="Source"+str(i)

    for h in range(1,variables_number+1):
        cc._add_input('input'+'_'+str(h+1),grouping=[0])
        graph.connect(start,'output',sources[h],'iterations')
        graph.connect(sources[h],'output',cc,'input'+'_'+str(h+1))
        
    
    graph.connect(cc,'output',mat,'input')
    graph.connect(mat,'output',mc,'input')
    
        
  
    return graph
        




print ("Preparing for: "+str(iterations/batch_size)+" projections" )


#Store via recorders or sensors
#ProvenanceRecorder.REPOS_URL='http://127.0.0.1:8080/j2ep-1.0/prov/workflow/insert'


#Store via recorders or sensors
#ProvenanceRecorder.REPOS_URL='http://127.0.0.1:8080/j2ep-1.0/prov/workflow/insert'

#Store via service
ProvenancePE.REPOS_URL='http://127.0.0.1:8082/workflow/insert'

#Export data lineage via service (REST GET Call on dataid resource)
ProvenancePE.PROV_EXPORT_URL='http://127.0.0.1:8082/workflow/export/data/'

#Store to local path
ProvenancePE.PROV_PATH='./prov-files/'

#Size of the provenance bulk before sent to storage or sensor
ProvenancePE.BULK_SIZE=20

#ProvenancePE.REPOS_URL='http://climate4impact.eu/prov/workflow/insert'

def createGraphWithProv():
    
    graph=createWf()
    #Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder subtypes

    # Ranomdly generated unique identifier for the current run
    rid='CORR_LARGE_'+getUniqueId()

    
    # Finally, provenance enhanced graph is prepared:   
   
    #Initialise provenance storage end associate a Provenance type with specific components:
    profile_prov_run(graph,None, provImpClass=(ProvenancePE,),
                     username='aspinuso',
                     runId=rid,
                     input=[{'name':'variables_number','url':variables_number},
                            {'name':'sampling_rate','url':sampling_rate},
                            {'name':'batch_size','url':batch_size},
                            {'name':'iterations','url':iterations}],
                     w3c_prov=False,
                     description="provState",
                     workflowName="test_rdwd",
                     workflowId="xx",
                     componentsType= {'MaxClique':{'s-prov:type':(SingleInvocationStateful,),
                                                   'state_dep_port':'graph',
                                                   's-prov:prov-cluster':'knmi:stockAnalyser'},
                                      'CorrMatrix':{'s-prov:type':(MultiInvocationGrouped,),
                                                    's-prov:prov-cluster':'knmi:stockAnalyser'},
                                      'CorrCoef':{'s-prov:type':(SingleInvocationFlow,),
                                                    's-prov:prov-cluster':'knmi:Correlator'}},
                                      
                    save_mode='service')

    #
    return graph



#graph=createWf()
graph=createGraphWithProv()

