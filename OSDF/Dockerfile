# Use Ubuntu as the OS and bash
FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Create an OSDF user and grant all permissions. Needed for RPM template.
RUN adduser --disabled-password --gecos '' osdf && adduser osdf sudo

# Install all necessary dependencies for various components, need JDK for ant
RUN apt-get update && apt-get install -y build-essential \
					wget \
					git \
					curl \
					default-jdk \
					ant \
					nodejs \
					software-properties-common \
					rpm

# Now install ES
RUN wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/deb/elasticsearch/2.3.1/elasticsearch-2.3.1.deb && dpkg -i elasticsearch-2.3.1.deb

# & CouchDB
RUN add-apt-repository ppa:couchdb/stable && apt-get update && apt-get install -y couchdb

#  Prepare the OSDF area for building and use ant/rpm to build
RUN su osdf -c "mkdir -p /home/osdf/api" && su osdf -c "git clone https://github.com/jmatsumura/OSDF.git /home/osdf/api"
RUN cd /home/osdf/api && su osdf -c "ant rpm"
RUN su osdf -c "rpmbuild /home/osdf/api/dist/osdf*.rpm"

# Expose ports and start the daemon (final step in RPM build)
EXPOSE 5984 9200 9300
CMD ["/bin/sh", "-c", "while true; do echo hello world; sleep 3; done"]
