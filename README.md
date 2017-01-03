# iHMPDCC_new_fxns

This repository will contain various files that will serve to supplement the functionality of the
iHMP site (http://ihmpdcc.org/resources/osdf.php).

The directories & files present contain the following...

FE - Dockerfile to build the front-end website.

BE - Python Flask app that runs the back-end for the GDC front. Uses GraphQL and Neo4j. Contains a Dockerfile similar to FE.

OSDF_to_Neo4j - Scripts for converting OSDF (CouchDB) into Neo4j.

docker_compose - Directory for building either just the BE or the FE+BE via docker-compose.

misc - Contains files like the conf for Neo4j that can be easily copied for personal use. 

## Original front-end/back-end implementations

GDC: https://github.com/NCI-GDC/portal-ui

OSDF: https://github.com/IGS/OSDF

## Setup

With Docker installed, build the containers using:

	1) Move to the directory containing the Dockerfile under ~FE/Docker/~ directory

	2) Run the following commands:

		A) docker build . -t fe

		B) docker run --name fe_con -p 3000:3000 -p 3001:3001 -p 9876:9876 fe
