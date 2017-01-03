# iHMPDCC_new_fxns

This repository will contain various files that will serve to supplement the functionality of the
iHMP site (http://ihmpdcc.org/resources/osdf.php).

The directories & files present contain the following...

GDC - Dockerfile to build the GDC front-end website.

GQL_OSDF - Python Flask app that runs the back-end for the GDC front. Uses GraphQL and Neo4j.

OSDF_to_Neo4j - Scripts for converting OSDF (CouchDB) into Neo4j.

misc - Contains files like the conf for Neo4j that can be easily copied for personal use. 

## Original front-end/back-end implementations

GDC: https://github.com/NCI-GDC/portal-ui

OSDF: https://github.com/IGS/OSDF

## Setup

With Docker installed, build these containers using either:

If you are using a different OS, currently you can only build the local UI (local ElasticSearch 
and local API not tested/supported) like so:

	1) Move to the directory containing the Dockerfile under GDC directory

	2) Run the following commands:

		A) docker build . -t gdc

		B) docker run --name gdc_con -p 3000:3000 -p 3001:3001 -p 9876:9876 gdc
