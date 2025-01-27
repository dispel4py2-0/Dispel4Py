# dispel4py

dispel4py is a free and open-source Python library for describing abstract stream-based workflows for distributed data-intensive applications. It enables users to focus on their scientific methods, avoiding distracting details and retaining flexibility over the computing infrastructure they use.  It delivers mappings to diverse computing infrastructures, including cloud technologies, HPC architectures and  specialised data-intensive machines, to move seamlessly into production with large-scale data loads. The dispel4py system maps workflows dynamically onto multiple enactment systems, and supports parallel processing on distributed memory systems with MPI and shared memory systems with multiprocessing, without users having to modify their workflows.

## Dependencies

dispel4py has been tested with Python *2.7.6*, *2.7.5*, *2.7.2*, *2.6.6* and Python *3.4.3*, *3.6*, *3.7*, *3.10*.

The following Python packages are required to run dispel4py:

- networkx (https://networkx.github.io/)

If using the MPI mapping:

- mpi4py (http://mpi4py.scipy.org/)

## Installation

Clone this repository to your desktop. You can then install from the local copy to your python environment by calling:

```
python setup.py install
```

from the dispel4py root directory.

## Docker

The Dockerfile in the dispel4py root directory builds a Debian Linux distribution and installs dispel4py and OpenMPI.

```
docker build . -t dare-dispel4py
```

Start a Docker container with the dispel4py image in interactive mode with a bash shell:

```
docker run -it dare-dispel4py /bin/bash
```

For the EPOS use cases obspy is included in a separate Dockerfile `Dockerfile.seismo`:

```
docker build . -f Dockerfile.seismo -t dare-dispel4py-seismo
```

## Development
For installing for development with a conda environment

### Installing
1. `conda create --name py7 python=3.7`
2. `conda activate py37`
3. `pip uninstall py37`
4. `git clone https://github.com/dispel4py2-0/dispel4py.git`
5. `cd dispel4py`
6. `pip install -r requirements.txt`
7. `python setup.py install`

### Examples
#### Word Count Example

RDD:
```sh
python -m dispel4py.new.processor dispel4py.new.dynamic_redis_v1 dispel4py.examples.graph_testing.word_count -ri localhost -n 4 -i 10
```

FD - This is the one that works for groupings!!

```sh
python -m dispel4py.new.processor dispel4py.new.dynamic_redis dispel4py.examples.graph_testing.word_count -ri localhost -n 4 -i 10
```

Note: In another tab, we need to have REDIS working in background: >> redis-server

---

#### Internal Extinction (dispel4py/examples/internal_extinction)

RDD:
```sh
python -m dispel4py.new.processor  dispel4py.new.dynamic_redis_v1 dispel4py.examples.internal_extinction.int_ext_graph  -ri localhost -n 4 -d "{\"read\" : [ {\"input\" : \"dispel4py/examples/internal_extinction/coordinates.txt\"} ]}"
```

FD -This is the one that works for groupings!!

**This doesn't seem to be working**
```sh
python -m dispel4py.new.processor  dispel4py.new.dynamic_redis dispel4py.examples.internal_extinction.int_ext_graph  -ri localhost -n 4 -d "{\"read\" : [ {\"input\" : \"dispel4py/examples/internal_extinction/coordinates.txt\"} ]}"
```

Note: In another tab, we need to have REDIS working in background: >> redis-server

---

#### Sentiment Analysis (dispel4py/examples/article_sentiment_analysis)

FD Strategy:
```sh
python -m dispel4py.new.processor  dispel4py.new.dynamic_redis dispel4py.examples.article_sentiment_analysis.analysis_sentiment -ri localhost -n 32 -d "{\"read\" : [ {\"input\" : \"dispel4py/examples/articles_sentiment_analysis/Articles_cleaned.csv\"} ]}
```

Note: In another tab, we need to have REDIS working in background: >> redis-server

