# Use Ubuntu as the OS and bash
FROM ubuntu:14.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Install Apache and all necessary dependencies for various
# components of GDC code
RUN sudo apt-get update && sudo apt-get install -y apache2 \
					git \
					nodejs \
					npm \
					python-pip \
					libpq-dev \
					python-dev \
					python3

# Use pip to install avro and graphviz dependencies
RUN pip install graphviz \
		avro \
		SQLAlchemy \
		Psycopg2

# Install each part of GDC separately. Note that the setups will 
# need to be executed outside of this file.
RUN mkdir -p /home/gdc/{dp,la,dtt,dd,dm,psqlg} 

# Data Portal
RUN git clone https://github.com/NCI-GDC/portal-ui.git /home/gdc/dp

# Legacy Archive
RUN git clone https://github.com/NCI-GDC/portal-ui-legacy.git /home/gdc/la

# Data Transfer Tool
RUN git clone https://github.com/NCI-GDC/gdc-client.git /home/gdc/dtt

# Data Dictionary
RUN git clone https://github.com/NCI-GDC/gdcdictionary.git /home/gdc/dd

# Data Model (layer between Data Dictionary and psqlgraph
RUN git clone https://github.com/NCI-GDC/gdcdatamodel.git /home/gdc/dm

# psqlgraph
RUN git clone https://github.com/NCI-GDC/psqlgraph.git /home/gdc/psqlg

# Expose ports and start the Apache server
EXPOSE 80
CMD ["/usr/sbin/apache2ctl", "-DFOREGROUND"]
