FROM quay.io/pawsey/mpich-base:3.4.3_ubuntu20.04

MAINTAINER Cormac Reynolds <cormac.reynolds@csiro.au>

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/difx/DiFX-2.6.3/lib
ENV PATH=${PATH}:/difx/DiFX-2.6.3/bin
ENV PERL5LIB=/difx/DiFX-2.6.3/share/perl/5.30.0:/difx/DiFX-2.6.3/./lib/x86_64-linux-gnu/perl/5.30.0
WORKDIR /difx/

RUN apt-get update \
    && apt-get install -y \
        tzdata \
        autoconf \
        automake \
        libtool \
        pkg-config \
        g++ \
        gcc \
        gfortran \
        make \
        python3 \
        python3-pip \
        libgsl23 \
        libgsl-dev \
        libexpat1-dev \
        bison \
        doxygen \
        python3-tk \
        vim \
        openssh-client \
        libfftw3-dev \
        build-essential \
        autotools-dev \
        flex \
        subversion \
    && ln -fs /usr/share/zoneinfo/Australia/Perth /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3 1 \
    && apt-get clean all \
    && rm -rf /var/lib/apt/lists/*

#ENV TZ=UTC
#ENV TZ=Australia/Perth
RUN pip3 install astropy requests numpy simplejson psutil matplotlib

COPY ipp ipp
RUN /difx/ipp/l_ipp_oneapi_p_2021.6.0.626_offline.sh -a -s --eula accept

COPY DiFX-2.6.3 DiFX-2.6.3
COPY espresso espresso
RUN source ./DiFX-2.6.3/setup.bash \
    && mkdir /difx/build ; cd /difx/build ; /difx/DiFX-2.6.3/install-difx --perl ; rm -rf /difx/build \
    && cd /difx/espresso; ./install.py $DIFXROOT
