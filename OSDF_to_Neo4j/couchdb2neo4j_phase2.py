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
from dicts_for_couchdb2neo4j import nodes, edges

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Skip any nested dictionaries like those under 'doc' or 'meta'. 'linkage' is
# skipped since this script is only concerned with creating nodes, not edges.
# Also skip numerous CouchDB specific attributes (_rev, rev, key, _id). 
skipUs = ['value','doc','meta','linkage','sequenced_from','acl','_rev','rev','key','_id']
skip = set(skipUs) 
e = set(edges)

# Recurse through JSON object. Note that throughout this function many nodes are
# likely to be created per document depending on the number of unique tags found.
def traverse_json(x, snode):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if k in e: # skip info we don't want to transfer and edge info for now
                pass
            elif k == "tags": # new node for each new tag in this list
                for tag in v:
                    cstr = "MERGE (node:Tags { term:'%s' })" % (tag)
                    #cypher.run(cstr)
            elif k == "mimarks" or k == 'mixs':
                for key,value in v.iteritems():
                    if value == "" or not value: # check for empty string/list
                        pass
                    else:
                        if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                            for z in value:
                                cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,z)
                                #cypher.run(cstr)
                        else:
                            cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,value)
                            #cypher.run(cstr)

        return max(traverse_json(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Some terminal feedback
print "Approximate number of documents found in CouchDB (likely includes _hist entries which are ignored) = %s" % (len(docList))
m = 0
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize array at each new document
        res = traverse_json(x, singleNode)
        props = ' , '.join(['%s:"%s"' % (key, value) for (key, value) in res.items()])
        cstr = "MERGE (node:`%s` { %s })" % (nodes[res['node_type']],props)
        #cypher.run(cstr)
        if m % 500 == 0:
            print "%s documents, which have been converted into nodes, now have edges attached in Neo4j" % (m)
        m += 1

print "Finished. Processed a total of %s documents." % (m)
