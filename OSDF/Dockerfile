# Use Ubuntu as the OS and bash
FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Install all necessary dependencies for various components, need JDK for ant
RUN apt-get update && apt-get install -y build-essential \
					wget \
					git \
					curl \
					default-jdk \
					ant \
					nodejs \
					software-properties-common \
					deb \
					rpm

# Now install ES
RUN wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/deb/elasticsearch/2.3.1/elasticsearch-2.3.1.deb && dpkg -i elasticsearch-2.3.1.deb

# & CouchDB
RUN add-apt-repository ppa:couchdb/stable && apt-get update && apt-get install -y couchdb

# Install docker-rpm-builder
RUN curl https://www.franzoni.eu/keys/D1270819.txt | sudo apt-key add - \
	&& echo "deb http://www.a9f.eu/apt/docker-rpm-builder-v1/ubuntu trusty main" > /etc/apt/sources.list.d/docker-rpm-builder.list

RUN apt-get update && apt-get install -y docker-rpm-builder

#  Prepare the OSDF area for building and use ant/rpm to build
RUN mkdir -p /home/osdf/api
RUN git clone https://github.com/jmatsumura/OSDF.git /home/osdf/api
#RUN cd /home/osdf/api && ant prepare \
#						&& ant dist \
#						&& ant rpm_prep \
#						&& ant rpm

# Expose ports and start the daemon (final step in RPM build)
EXPOSE 5984 9200 9300
#CMD ["bash", "-c", "/home/osdf/api/rpm/init.sh"]
