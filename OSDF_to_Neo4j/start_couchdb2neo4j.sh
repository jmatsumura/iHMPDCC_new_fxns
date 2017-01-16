#!/bin/bash

# Check if the script is not running
if [[ ! $(ps aux | grep 'mirror_couchdb2neo4j' | grep -v grep) ]]; then
    # If not running, start it
    ./mirror_couchdb2neo4j_changesfeed.py ./mirror.conf
fi
