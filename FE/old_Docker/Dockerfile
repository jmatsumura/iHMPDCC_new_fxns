# Use Ubuntu as the OS and bash
FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Note that anything commented out within a RUN command is likely used
# to install the other components of the GDC code that isn't the data portal.

# Install all necessary dependencies for various components
RUN apt-get update && apt-get install -y git \
					build-essential \
					libkrb5-dev \
					nodejs \
					npm \
					nodejs-legacy
#					python-pip \
#					libpq-dev \
#					python-dev \
#					python3 \
#					libxml2-dev \
#					libxslt1-dev \
#					libyaml-dev \
#					libfontconfig

# Use pip to install various dependencies for DTT, DD, DM, and Psqlgraph.
#RUN pip install graphviz \
#		avro \
#		SQLAlchemy \
#		Psycopg2 \
#		pyyaml \
#		lxml

# Install each part of GDC separately. 
RUN mkdir -p /home/gdc/{dp,la,dtt,dd,dm,psqlg}

# The environment requires xterm for what is output by the setup scripts
# and NPM must be updated/cleaned to fix numerous errno34 alerts.
ENV TERM=xterm

# Data Portal
RUN git clone https://github.com/jmatsumura/portal-ui.git /home/gdc/dp
RUN cd /home/gdc/dp && ./setup.sh

# Data Transfer Tool
#RUN git clone https://github.com/NCI-GDC/gdc-client.git /home/gdc/dtt
#RUN cd /home/gdc/dtt && python ./setup.py install

# Data Dictionary
#RUN git clone https://github.com/NCI-GDC/gdcdictionary.git /home/gdc/dd
#RUN cd /home/gdc/dd && python ./setup.py install

# Data Model (layer between Data Dictionary and psqlgraph
#RUN git clone https://github.com/NCI-GDC/gdcdatamodel.git /home/gdc/dm
#RUN cd /home/gdc/dm && python ./setup.py install

# psqlgraph
#RUN git clone https://github.com/NCI-GDC/psqlgraph.git /home/gdc/psqlg
#RUN cd /home/gdc/psqlg && python ./setup.py install

# Legacy Archive
#RUN git clone https://github.com/NCI-GDC/portal-ui-legacy.git /home/gdc/la
#RUN cd /home/gdc/la && ./setup.sh

# Expose ports and start nginx service
# Ports are noted by: nginx conf, npm start, npm start, karma start
EXPOSE 3000 3001 9876
#CMD [ "bash", "-c", "cd /home/gdc/dp; GDC_API='https://gdc-portal.nci.nih.gov/auth/api/v0' GDC_FAKE_AUTH=true npm start"]
CMD [ "bash", "-c", "cd /home/gdc/dp; GDC_API='http://localhost:5000' GDC_FAKE_AUTH=true npm start"]
