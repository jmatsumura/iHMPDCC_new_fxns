# Use Ubuntu as the OS and bash
FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Install first set of necessary dependencies. Note that
# build-essential will be grabbing python2.7 and pip
RUN apt-get update && apt-get install -y git \
					build-essential \
					python2.7 \
					python-pip

# Install python-specific depdencies
RUN pip install flask \
					Flask-GraphQL \
					"graphene>=1.0" \
					py2neo

# Install the backend of iHMP
RUN mkdir -p /home/ihmp
RUN git clone https://github.com/jmatsumura/iHMPDCC_new_fxns.git /home/ihmp
RUN mv /home/ihmp/docker_compose/BE_only/models.py /home/ihmp/BE/.

# Expose ports and start flask service
EXPOSE 5000
CMD python /home/ihmp/BE/app.py
