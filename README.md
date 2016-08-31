# docker_GDC

This repository will contain a docker-compose.yml file that will build a set of Docker containers
to run the GDC code along with ElasticSearch and their API. The code is based on an Ubuntu 16.04 
environment and ElasticSearch will use version 5.0.

With Docker installed, build these containers like so:

1) Move to the directory containing the docker-compose.yml file

2) Run the following commands:

	A) docker-compose up -d
