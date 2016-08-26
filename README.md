# docker_GDC

This repository will contain the Dockerfile and anything else needed in 
order to build the base GDC code in an Ubuntu 14.04 environment. 

With Docker installed, use this file like so:

1) Go to the directory containing the Dockerfile

2) Run the following commands:

	A) docker build . -t gdc
	B) docker run --name gdc_con -p 80:80 -p 3000:3000 -p 3001:3001 -p 9876:9876 gdc
