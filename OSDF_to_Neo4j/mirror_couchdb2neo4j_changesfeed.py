#!/usr/bin/python
#
# Script to go through a CouchDB changes feed of OSDF data update the respective
# nodes in Neo4j.
#
# Accepts the following parameter:
# 1) Path to couchdb_changes_feed.json file
#

import json, sys, re
from py2neo import Graph
from accs_for_flattened_couchdb2neo4j import nodes, edges, body_site_dict, mod_quotes

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['results']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Skip any nested dictionaries like those under 'doc' or 'meta'. 'linkage' is
# skipped since this script is only concerned with creating nodes, not edges.
# Also skip numerous CouchDB specific attributes (_rev, rev, key, _id). 
skipUs = ['value','doc','meta','acl','_rev','rev','key','_id','_search']
skip = set(skipUs) 

# Recurse through JSON object. Note that throughout this function many nodes are
# likely to be created per document depending on the number of unique tags found.
def traverse_json(x, snode):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "" or not v: # check for empty string/list 
                continue
            else: 
                # Tags (list), MIMARKS (dict), and mixs (dict), should be individual nodes so add now
                if k == "tags": # new node for each new tag in this list
                    for tag in v:
                        tag = mod_quotes(tag)
                        cstr = "MERGE (node:Tags { term:'%s' })" % (tag)
                        #cypher.run(cstr)
                elif k == "mimarks" or k == 'mixs':
                    for key,value in v.iteritems():
                        if value == "" or not value: # check for empty string/list
                            continue
                        else:
                            if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                                for z in value:
                                    z = mod_quotes(z)
                                    cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,z)
                                    #cypher.run(cstr)
                            else:
                                value = mod_quotes(value)
                                cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,value)
                                #cypher.run(cstr)

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

# Need a hash to catch updated versions of a doc since OSDF keeps a history of that
versions = {}
regex_for_id = r'`id`:"([a-zA-Z0-9]*)"'
regex_for_ver = r'`ver`:(\d+)'

# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None and re.match(r'\_design.*', x['id']) is None: # ignore history/design documents
        if 'deleted' in x:
            if x['deleted'] == True:
                cstr = "MATCH (n) WHERE n.id='%s' DETACH DELETE n" % (x['id'])
                #print cstr
        if 'linkage' in x['doc']:
            for edge in edges:
                if edge in x['doc']['linkage']:
                    cstr = "MATCH (n1{`id`:'%s'}),(n2{`id`:'%s'}) CREATE (n1)-[:%s]->(n2)" % (x['id'],x['doc']['linkage'][edge][0],edges[edge])
                    print cstr
               