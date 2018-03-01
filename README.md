dispel4py
=========

<img align="right" alt="dispel4py logo" width="350" src="http://dispel4py.org/images/DISPEL4PY_web.jpg">
dispel4py is a free and open-source Python library for describing abstract stream-based workflows for distributed data-intensive applications. It enables users to focus on their scientific methods, avoiding distracting details and retaining flexibility over the computing infrastructure they use.  It delivers mappings to diverse computing infrastructures, including cloud technologies, HPC architectures and  specialised data-intensive machines, to move seamlessly into production with large-scale data loads. The dispel4py system maps workflows dynamically onto multiple enactment systems, such as MPI, STORM and Multiprocessing, without users having to modify their workflows.

Dependencies 
------------

dispel4py has been tested with Python *2.7.6*, *2.7.5*, *2.7.2*, *2.6.6* and Python *3.4.3*.

The following Python packages are required to run dispel4py:

- networkx (https://networkx.github.io/)

If using the MPI mapping:

- mpi4py (http://mpi4py.scipy.org/)

If using the Storm mapping:

- Python Storm module, available here: https://github.com/apache/storm/tree/master/storm-multilang/python/src/main/resources/resources, to be placed in directory `resources`.
- Python Storm thrift generated code, available here: https://github.com/apache/storm/tree/master/storm-core/src/py

![Logo](http://dispel4py.org/images/DISPEL4PY_web.jpg)

