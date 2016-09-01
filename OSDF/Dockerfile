# Use Ubuntu as the OS and bash
FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Install all necessary dependencies for various components, need JDK for ant
RUN apt-get update && apt-get install -y git \
					wget \
					build-essential \
					default-jdk \
					ant \
					nodejs \
					software-properties-common \
					rpm

#  Prepare the OSDF area for building and use ant/rpm to build
RUN mkdir -p /home/osdf/api
RUN git clone https://github.com/IGS/OSDF.git /home/osdf/api
RUN cd /home/osdf/api && ant all && rpmbuild ./rpm/osdf.spec.template 

# Now install ES
RUN wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/deb/elasticsearch/2.3.1/elasticsearch-2.3.1.deb && dpkg -i elasticsearch-2.3.1.deb

# & CouchDB
RUN add-apt-repository ppa:couchdb/stable && apt-get update && apt-get install couchdb

EXPOSE 5984 9200 9300
