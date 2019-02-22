FROM debian:latest

ENV PATH /opt/conda/bin:$PATH

RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 \
    build-essential openmpi-bin \
    git vim nano screen

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh

ENV USER dare
ENV HOME /home/dare
ENV CONDA_ENV_NAME dispel4py

RUN adduser ${USER} --disabled-password --gecos ""

USER ${USER}
WORKDIR ${HOME}

RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate ${CONDA_ENV_NAME}" >> ~/.bashrc

RUN conda create -y --name ${CONDA_ENV_NAME}

ENV PATH ${HOME}/.conda/envs/${CONDA_ENV_NAME}/bin:$PATH
ENV CONDA_DEFAULT_ENV ${CONDA_ENV_NAME}
ENV CONDA_PREFIX ${HOME}/.conda/envs/${CONDA_ENV_NAME}

RUN conda install -y -c anaconda mpi4py
RUN conda install -y jupyter numpy

# install dispel4py latest
RUN git clone https://gitlab.com/project-dare/dispel4py.git && \
    cd dispel4py && python setup.py install
