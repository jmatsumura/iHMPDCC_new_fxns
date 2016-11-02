#!/usr/bin/python
#
# Script to go through a CouchDB dump of OSDF data and create the respective
# nodes in Neo4j. 
#
# Accepts the following parameter:
# 1) Path to couchdb_dump.json file
#
# Phase 1 consists of simply converting all documents+attributes into
# nodes+properties. Phase 2 will add the relevant edges to the nodes. 

import json, sys, re
from py2neo import Graph
from accs_for_flattened_couchdb2neo4j import nodes, edges, mod_quotes

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Function to build an index in Neo4j to make edge connection a bit faster during
# the lookup phase. Accepts the name of a node (possible values in dicts_for_couchdb2neo4j)
# and the property that that node ought to be indexed by. 
def build_constraint_index(node,prop):
    cstr = "CREATE CONSTRAINT ON (x:%s) ASSERT x.%s IS UNIQUE" % (node,prop)
    cypher.run(cstr)

build_constraint_index('Case','id')
build_constraint_index('File','id')
build_constraint_index('Tags','term')

# Skip any nested dictionaries like those under 'doc' or 'meta'. 'linkage' is
# skipped since this script is only concerned with creating nodes, not edges.
# Also skip numerous CouchDB specific attributes (_rev, rev, key, _id). 
skipUs = ['value','doc','meta','linkage','sequenced_from','acl','_rev','rev','key','_id','_search','ver']
skip = set(skipUs) 
e = set(edges)

# Recurse through JSON object. Note that throughout this function many nodes are
# likely to be created per document depending on the number of unique tags found.
def traverse_json(x, snode):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "" or not v: # check for empty string/list 
                pass
            elif k in skip or k in e: # skip info we don't want to transfer and edge info for now
                pass
            else: 
                # Tags (list), MIMARKS (dict), and mixs (dict), should be individual nodes so add now
                if k == "tags": # new node for each new tag in this list
                    for tag in v:
                        tag = mod_quotes(tag)
                        cstr = "MERGE (node:Tags { term:'%s' })" % (tag)
                        cypher.run(cstr)
                elif k == "mimarks" or k == 'mixs':
                    for key,value in v.iteritems():
                        if value == "" or not value: # check for empty string/list
                            pass
                        else:
                            if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                                for z in value:
                                    z = mod_quotes(z)
                                    cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,z)
                                    cypher.run(cstr)
                            else:
                                value = mod_quotes(value)
                                cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,value)
                                cypher.run(cstr)

                else: # any attributes other than tags, mimarks, or mixs, process here

                    # A few keys need special handling due to their values not being strings
                    if k == "write":
                        v = v[0]
                    elif k == "read":
                        v = v[0]
                    elif k == "checksums":
                        v = v['md5']
                    elif k == "contact": # Note, building a single string of contacts out of potentially many
                        contacts = []
                        for i in v:
                            if "@" in i:
                                contacts.append(i)
                        v = (',').join(contacts)

                    if k == "urls":
                        prop = "" # variable prop name for each URL, shouldn't expect consistent ordering
                        for file in v:
                            if 'ftp://' in file:
                                prop = "ftp"
                            elif 'http://' in file:
                                prop = "http"
                            elif 's3://' in file:
                                prop = "s3"
                            elif 'fasp://' in file:
                                prop = "fasp"
                            if prop not in snode and prop != "": # ensure no dupes due to recursion
                                snode[prop] = file
                    else: 
                        if k not in snode and not isinstance(v, dict) and not isinstance(v, list): # reached single k/v pairs, add now
                            snode[k] = v

        return max(traverse_json(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Some terminal feedback
print "Approximate number of documents found in CouchDB (likely includes _hist entries which are ignored) = %s" % (len(docList))
n = 0
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize dict at each new document
        res = traverse_json(x, singleNode)
        props = ""
        y = 0 # track how many props are being added
        for key,value in res.iteritems():
            if key == 'fma_body_site':
                key = 'body_site' # standardize body site and fma_body_site across iHMP and HMP
                # need to change value here when I have the map from Heather
            if y > 0: # add comma for every subsequent key/value pair
                props += ',' 
            if isinstance(value, int) or isinstance(value, float):
                props += '`%s`:%s' % (key,value)
                y += 1
            else:
                value = mod_quotes(value)
                props += '`%s`:"%s"' % (key,value)
                y += 1
        if 'node_type' in res: # if no node type, need to ignore     
            cstr = "CREATE (node:`%s` { %s })" % (nodes[res['node_type']],props) # create should make this faster, trust CouchDB to guarantee unique
            cypher.run(cstr)
        if n % 1000 == 0:
            print "%s documents converted into nodes and in Neo4j" % (n)
        n += 1

print "Finished. Processed a total of %s documents." % (n)
