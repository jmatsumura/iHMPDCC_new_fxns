#!/usr/bin/python
#
# Script to go through a CouchDB dump of OSDF data and create the respective
# edges in Neo4j. Requires that couchdb2neo4j_phase1.py was run to completion
# since it expects all the nodes to be present before adding the appropriate edges.
#
# Phase 1 consists of simply converting all documents+attributes into
# nodes+properties. Phase 2 will add the relevant edges to the nodes. 

import json, sys, re
from py2neo import Graph
from dicts_for_couchdb2neo4j import nodes, edges, definitive_edges

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Make a set out of the known edges so we can create them when found. 
e = set(edges)
de = set (definitive_edges)

def build_index(node,prop):
    cstr = "CREATE INDEX ON: %s(%s)" % (node,prop)

# These indices will be used a lot and belong to edges where the endpoint,
# as well as the property to match to, is known. 
build_index('Project','id')
build_index('Study','id')
build_index('Subject','id')
build_index('Visit','id')
build_index('Sample','id')
build_index('Tags','term')

# Recurse through JSON object and find anything that pertains to an edge. 
def traverse_json(x, snode):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "": 
                pass
            elif k == "node_type" or k == "tags" or k == "id":
                if k not in snode:
                    snode[k] = v
            elif k == "mimarks" or k == 'mixs':
                for key,value in v.iteritems():
                    if value == "" or not value: # check for empty string/list
                        pass
                    else:
                        if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                            for z in value:
                                if isinstance(z, str):
                                    z = z.replace("'","\'")
                                    z = z.replace('"','\"')
                                combined = "%s:'%s'" % (key,z)
                                if combined not in snode[k]:
                                    snode[k].append(combined)
                        else:
                            if isinstance(value, str):
                                value = value.replace("'","\'")
                                value = value.replace('"','\"')
                            combined = "%s:'%s'" % (key,value)
                            if combined not in snode[k]:
                                snode[k].append(combined)

            elif k in e: # skip info we don't want to transfer and edge info for now
                pass

        return max(traverse_json(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Some terminal feedback
print "Beginning to add the edges to all docs within the file '%s'. Total number of docs (including _hist) is = %s" % (sys.argv[1],len(docList))
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
n = 0
tot = 0
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize dict at each new document
        singleNode['mimarks'] = []
        singleNode['mixs'] = []
        res = traverse_json(x, singleNode)
        #print "%s edges now present in Neo4j." % (n)
