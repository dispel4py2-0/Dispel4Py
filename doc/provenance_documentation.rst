provenance
==========

clean\_empty
------------

.. code:: python

    clean_empty(d)

Utility function that given a dictionary in input, removes all the
properties that are set to None. It workes recursevly through lists and
nested documents

total\_size
-----------

.. code:: python

    total_size(o, handlers={}, verbose=False)

Returns the approximate memory footprint an object and all of its
contents.

Automatically finds the contents of the following builtin containers and
their subclasses: tuple, list, deque, dict, set and frozenset. To search
other containers, add handlers to iterate over their contents:

handlers = {SomeContainerClass: iter, OtherContainerClass:
OtherContainerClass.get\_elements}

write
-----

.. code:: python

    write(self, name, data)

Redefines the native write function of the dispel4py SimpleFunctionPE to
take into account provenance payload when transfering data.

getDestination\_prov
--------------------

.. code:: python

    getDestination_prov(self, data)

When provenance is activated it redefines the native
dispel4py.new.process getDestination function to take into account
provenance information when redirecting grouped operations.

commandChain
------------

.. code:: python

    commandChain(commands, envhpc, queue=None)

Utility function to execute a chain of system commands on the hosting
oeprating system. The current environment variable can be passed as
parameter env. The queue parameter is used to store the stdoutdata,
stderrdata of each process in message

ProvenanceType
--------------

.. code:: python

    ProvenanceType(self)

A workflow is a program that combines atomic and independent processing
elements via a specification language and a library of components. More
advanced systems adopt abstractions to facilitate re-use of workflows
across users'' contexts and application domains. While methods can be
multi-disciplinary, provenance should be meaningful to the domain
adopting them. Therefore, a portable specification of a workflow
requires mechanisms allowing the contextualisation of the provenance
produced. For instance, users may want to extract domain-metadata from a
component or groups of components adopting vocabularies that match their
domain and current research, tuning the level of granularity. To allow
this level of flexibility, we explore an approach that considers a
workflow component described by a class, according to the
Object-Oriented paradigm. The class defines the behaviour of its
instances as their type, which specifies what an instance will do in
terms of a set of methods. We introduce the concept of *ProvenanceType*,
that augments the basic behaviour by extending the class native type, so
that a subset of those methods perform the additional actions needed to
deliver provenance data. Some of these are being used by some of the
preexisting methods, and characterise the behaviour of the specific
provenance type, some others can be used by the developer to easily
control precision and granularity. This approach, tries to balance
between automation, transparency and explicit intervention of the
developer of a data-intensive tool, who can tune provenance-awareness
through easy-to-use extensions.

The type-based approach to provenance collection provides a generic
*ProvenanceType* class that defines the properties of a provenance-aware
workflow component. It provides a wrapper that meets the provenance
requirements, while leaving the computational behaviour of the component
unchanged. Types may be developed as **Pattern Type** and **Contextual
Type** to represent respectively complex computational patterns and to
capture specific metadata contextualisations associated to the produce
output data.

The *ProvenanceType* presents the following class constants to indicate
where the lineage information will be stored. Options include a remote
repository, a local file system or a *ProvenanceSensor* (experimental).

-  *SAVE*\ MODE\_SERVICE='service'\_
-  *SAVE*\ MODE\_FILE='file'\_
-  *SAVE*\ MODE\_SENSOR='sensor'\_

The following variables will be used to configure some general
provenance capturing properties

-  *PROV*\ PATH\_: When *SAVE*\ MODE\_SERVICE\_ is chosen, this variable
   should be populated with a string indcating a file system path wher
   the lineage will be stored
-  *REPOS*\ URL\_: When *SAVE*\ MODE\_SERVICE\_ is chosen, this variable
   should be populated with a string indcating the repository endpoint
   (S-ProvFlow) where the provenance will be sent.
-  *PROV*\ DATA\_EXPORT\_URL: The service endpoint from where the
   provenance of a workflow execution, after being stored, can be
   extracted in PROV format.
-  *BULK*\ SIZE\_: Number of lineage documents to be stored in a single
   file or in a single request to the remote service. Helps tuning the
   overhead brough by the latency of accessing storage resources.

AccumulateFlow
--------------

.. code:: python

    AccumulateFlow(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) whose
output depends on a sequence of input data; e.g. computation of periodic
average.

Nby1Flow
--------

.. code:: python

    Nby1Flow(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) whose
output depends on the data received on all its input ports in lock-step;
e.g. combined analysis of multiple variables.

SlideFlow
---------

.. code:: python

    SlideFlow(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) whose
output depends on computations over sliding windows; e.g. computation of
rolling sums.

ASTGrouped
----------

.. code:: python

    ASTGrouped(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) that
manages a stateful operator with grouping rules; e.g. a component that
produces a correlation matrix with the incoming coefficients associated
with the same sampling-iteration index

SingleInvocationFlow
--------------------

.. code:: python

    SingleInvocationFlow(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) that
presents stateless input output dependencies; e.g. the Processing
Element of a simple I/O pipeline.

AccumulateStateTrace
--------------------

.. code:: python

    AccumulateStateTrace(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) that
keeps track of the updates on intermediate results written to the output
after a sequence of inputs; e.g. traceable approximation of frequency
counts or of periodic averages.

IntermediateStatefulOut
-----------------------

.. code:: python

    IntermediateStatefulOut(self)

A *Pattern type* for a Processing Element (*s-prov:Component*) stateful
component which produces distinct but interdependent output; e.g.
detection of events over periodic observations or any component that
reuses the data just written to generate a new product

ForceStateless
--------------

.. code:: python

    ForceStateless(self)

A *Pattern type* for a Processing Element (*s-prov:Component*). It
considers the outputs of the component dependent only on the current
input data, regardless from any explicit state update; e.g. the user
wants to reduce the amount of lineage produced by a component that
presents inline calls to the *update*\ prov\_state\_, accepting less
accuracy.

get\_source
-----------

.. code:: python

    get_source(object, spacing=10, collapse=1)

Print methods and doc strings. Takes module, class, list, dictionary, or
string.

injectProv
----------

.. code:: python

    injectProv(object, provType, active=True, componentsType=None, workflow={}, **kwargs)

This function dinamically extend the type of each the nodes of the graph
or subgraph with ProvenanceType type or its specialisation

configure\_prov\_run
--------------------

.. code:: python

    configure_prov_run(graph, provRecorderClass=None, provImpClass=<class 'provenance.ProvenanceType'>, input=None, username=None, workflowId=None, description=None, system_id=None, workflowName=None, workflowType=None, w3c_prov=False, runId=None, componentsType=None, clustersRecorders={}, feedbackPEs=[], save_mode='file', sel_rules={}, transfer_rules={}, update=False)

In order to enable the user of a data-intensive application to configure
the attribution of types, selectivity controls and activation of
advanced exploitation mechanisms, we introduce in this chapter also the
concept of provenance configuration. In Figure 4.1 we outline the
different phases envisaged by framework. In that respect, we propose a
configuration profile, where users can specify a number of properties,
such as attribution, provenance types, clusters, sensors, selectivity
rules, etc. The configuration is used at the time of the initialisation
of the workflow to prepare its provenance-aware execution. We consider
that a chosen configuration may be influenced by personal and community
preferences, as well as by rules introduced by institutional policies.
For instance, a Research Infrastructure (RI) may indicate best practices
to reproduce and describe the operations performed by the users
exploiting its facilities, or even impose requirements which may turn
into quality assessment metrics. For instance, a certain RI would
require to choose among a set of contextualisation types, in order to
adhere to the infrastructure’s metadata portfolio. Thus, a provenance
configuration profile play in favour of more generality, encouraging the
implementation and the re-use of fundamental methods across disciplines.

With this method, the users of the workflow provide general provenance
information on the attribution of the run, such as *username*, *runId*
(execution id), *description*, *workflowName*, and its semantic
characterisation *workflowType*. It allows users to indicate which
provenance types to apply to each component and the belonging conceptual
provenance cluster. Moreover, users can also choose where to store the
lineage (*save*\ mode\_), locally in the file system or in a remote
service or database. Lineage storage operations can be performed in
bulk, with different impacts on the overall overhead and on the
experienced rapidity of access to the lineage information.

-  **Selectivity and Transfer rules**: By declaratively indicating a set
   of Selectivity and Transfer rules for every component
   (*sel*\ rules\_, *transfer*\ rules\_), users can respectively
   activate the collection of the provenance for particular Data
   elements or trigger transfer operations of the data to external
   locations. The approach takes advantage of the contextualisation
   possibilities offered by the provenance *Contextualisation types*.
   The rules consist of comparison expressions formulated in JSON that
   indicate the boundary values for a specific metadata term. Such
   representation is inspired by the query language and selectors
   adopted by a popular document store, MongoDB.

Example, a Processing Element *CorrCoef* that produces lineage
information only when the *rho* value is greater than 0:

.. code:: python

        { "CorrCoef": {
            "rules": {
                "rho": {
                    "$gt": 0
        }}}}

ProvenanceSimpleFunctionPE
--------------------------

.. code:: python

    ProvenanceSimpleFunctionPE(self, *args, **kwargs)

A *Pattern type* for the native *SimpleFunctionPE* of dispel4py

ProvenanceIterativePE
---------------------

.. code:: python

    ProvenanceIterativePE(self, *args, **kwargs)

A *Pattern type* for the native *IterativePE* Element of dispel4py

ProvenanceRecorder
------------------

.. code:: python

    ProvenanceRecorder(self, name='ProvenanceRecorder', toW3C=False)

