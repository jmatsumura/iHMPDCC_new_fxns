# docker_GDC

This repository will contain a docker-compose.yml file that will build a set of Docker containers
to run the GDC code along with ElasticSearch and their API. The code is based on an Ubuntu 16.04 
environment and ElasticSearch will use version 5.0.

With Docker installed, build these containers using either:


If you are in a Linux environment, docker-compose is readily supported:

	1) Move to the directory containing the docker-compose.yml file

	2) Run the following commands:

		A) docker-compose up -d


If you are using a different OS, currently you can only build the local UI (local ElasticSearch 
and local API not tested/supported) like so:

	1) Move to the directory containing the Dockerfile under GDC directory

	2) Run the following commands:

		A) docker build . -t gdc

		B) docker run --name gdc_con -p 80:80 -p 3000:3000 -p 3001:3001 -p 9876:9876 gdc
