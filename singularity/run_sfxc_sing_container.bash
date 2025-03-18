#!/bin/bash
docker image remove -f "$(docker images -a -q)"
docker rm -v -f "$(docker ps -qa)"
rm sfxc_ipp.simg
docker build -t sfxc_ipp:latest -f sfxc_docker.def .
docker run -d -p 5000:5000 --restart=always --name registry registry:2
docker tag sfxc_ipp localhost:5000/sfxc_ipp
docker push localhost:5000/sfxc_ipp
SINGULARITY_NOHTTPS=1 singularity build sfxc_ipp.simg sfxc_ipp_singularity.def
