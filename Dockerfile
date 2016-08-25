# Use Ubuntu as the OS and bash
FROM ubuntu:14.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

MAINTAINER James Matsumura (jmatsumura@som.umaryland.edu)

# Install Apache and all necessary dependencies for various
# components of GDC code
RUN sudo apt-get update && sudo apt-get install -y apache2 \
					git \
					nodejs-legacy \
					npm \
					python-pip \
					libpq-dev \
					python-dev \
					python3 \
					libxml2-dev \
					libxslt1-dev

# Use pip to install avro and graphviz dependencies
RUN pip install graphviz \
		avro \
		SQLAlchemy \
		Psycopg2 \
		pyyaml \
		lxml

# Install each part of GDC separately. Note that the setups will 
# need to be executed outside of this file.
RUN mkdir -p /home/gdc/{dp,la,dtt,dd,dm,psqlg} 
ENV TERM=xterm
RUN npm install -g npm@latest
RUN npm cache clean

# Data Portal
RUN git clone https://github.com/NCI-GDC/portal-ui.git /home/gdc/dp
RUN cd /home/gdc/dp && ./setup.sh

# Legacy Archive
RUN git clone https://github.com/NCI-GDC/portal-ui-legacy.git /home/gdc/la
RUN cd /home/gdc/la && ./setup.sh

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

# Expose ports and start the Apache server
EXPOSE 80
CMD ["/usr/sbin/apache2ctl", "-DFOREGROUND"]
