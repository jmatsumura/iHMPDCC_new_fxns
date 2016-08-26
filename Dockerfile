# Use Ubuntu as the OS and bash
FROM ubuntu:14.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Install all necessary dependencies for various components
RUN sudo apt-get update && sudo apt-get install -y git \
					nodejs-legacy \
					npm \
					python-pip \
					libpq-dev \
					python-dev \
					python3 \
					libxml2-dev \
					libxslt1-dev \
					libyaml-dev

# Use pip to install various dependencies for DTT, DD, DM, and Psqlgraph.
RUN pip install graphviz \
		avro \
		SQLAlchemy \
		Psycopg2 \
		pyyaml \
		lxml

# Install each part of GDC separately. 
RUN mkdir -p /home/gdc/{dp,la,dtt,dd,dm,psqlg}

# The environment requires xterm for what is output by the setup scripts
# and NPM must be updated/cleaned to fix numerous errno34 alerts.
ENV TERM=xterm
RUN npm install -g npm@latest && npm cache clean
RUN npm install -g bower@latest && npm cache clean
RUN npm install -g typings@latest && npm cache clean
RUN npm install -g gulp-cli@latest && npm cache clean

# Data Portal
RUN git clone https://github.com/jmatsumura/portal-ui.git /home/gdc/dp
RUN cd /home/gdc/dp && ./setup.sh && typings install

# Data Transfer Tool
RUN git clone https://github.com/NCI-GDC/gdc-client.git /home/gdc/dtt
RUN cd /home/gdc/dtt && python ./setup.py install

# Data Dictionary
RUN git clone https://github.com/NCI-GDC/gdcdictionary.git /home/gdc/dd
RUN cd /home/gdc/dd && python ./setup.py install

# Data Model (layer between Data Dictionary and psqlgraph
RUN git clone https://github.com/NCI-GDC/gdcdatamodel.git /home/gdc/dm
RUN cd /home/gdc/dm && python ./setup.py install

# psqlgraph
RUN git clone https://github.com/NCI-GDC/psqlgraph.git /home/gdc/psqlg
RUN cd /home/gdc/psqlg && python ./setup.py install

# Legacy Archive
#RUN git clone https://github.com/NCI-GDC/portal-ui-legacy.git /home/gdc/la
#RUN cd /home/gdc/la && ./setup.sh

# Expose ports and start nginx service
# Ports are noted by: nginx conf, npm start, npm start, karma start
EXPOSE 80 3000 3001 9876
CMD [ "bash", "-c", "cd /home/gdc/dp; GDC_API=https://gdc-portal.nci.nih.gov/auth/api/v0 GDC_FAKE_AUTH=true npm start"]
