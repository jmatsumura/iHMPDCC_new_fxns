#!/usr/bin/python
#
# Script to go through a CouchDB dump of OSDF data and create the respective
# nodes in Neo4j. 
#
# Accepts the following parameter:
# 1) Path to couchdb_dump.json file
#
### PLEASE NOTE ###
# After this script finishes, an output file will be written to the directory the script
# was invoked in. This file needs to be passed to the neo4j-shell for loading using the
# following command (note make sure the database is as you want it to be as this will
# be using largely CREATE statements, not MERGE, so it is likely to double the data if 
# you are not starting with a new database):
# /path/to/neo4j/bin/neo4j-shell -path /data/databases/my_graph.db -c < /couchdb2neo4.phase1.out
#
# Phase 1 consists of simply converting all documents+attributes into
# nodes+properties. Phase 2 will add the relevant edges to the nodes. 

import json, sys, re
from dicts_for_couchdb2neo4j import nodes, edges, definitive_edges

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

o = open('./couchdb2neo4j.cypher','w')

#########
# NODES #
#########

# Function to build an index in Neo4j to make edge connection a bit faster during
# the lookup phase. Accepts the name of a node (possible values in dicts_for_couchdb2neo4j)
# and the property that that node ought to be indexed by. 
def build_index(node,prop,output):
    cstr = "CREATE INDEX ON: `%s`(%s);\n" % (node,prop)
    output.write(cstr+'\n')

o.write('BEGIN'+'\n')
# These indices will be used a lot and belong to edges where the endpoint,
# as well as the property to match to, is known. 
noId = ['tags','mimarks','mixs']
for k,v in nodes.iteritems(): # build indices on primary key of all those nodes that have one
    if k not in noId:
        build_index(v,'id',o)
build_index('Tags','term',o)
o.write('COMMIT'+'\n')
o.write('BEGIN'+'\n')

# Skip any nested dictionaries like those under 'doc' or 'meta'. 'linkage' is
# skipped since this script is only concerned with creating nodes, not edges.
# Also skip numerous CouchDB specific attributes (_rev, rev, key, _id). 
skipUs = ['value','doc','meta','linkage','sequenced_from','acl','_rev','rev','key','_id','_search']
skip = set(skipUs) 
e = set(edges)

# Recurse through JSON object. Note that throughout this function many nodes are
# likely to be created per document depending on the number of unique tags found.
def traverse_json_nodes(x, snode):
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
                        cstr = "MERGE (node:Tags { term:'%s' })" % (tag)
                        o.write(cstr+'\n')
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
                                    cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,z)
                                    o.write(cstr+'\n')

                            else:
                                if isinstance(value, str):
                                    value = value.replace("'","\'")
                                    value = value.replace('"','\"')
                                cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,value)
                                o.write(cstr+'\n')

                else: # any attributes other than tags, mimarks, or mixs, process here

                    # A few keys need special handling due to their values not being strings
                    if k == "write":
                        v = v[0]
                    elif k == "read":
                        v = v[0]
                    elif k == "checksums":
                        v = v['md5']
                    elif k == "contact": # Note, building a single string out of potentially many here for now
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

        return max(traverse_json_nodes(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json_nodes(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Some terminal feedback
print "Approximate number of documents found in CouchDB (likely includes _hist entries which are ignored) = %s" % (len(docList))
n = 0
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize dict at each new document
        res = traverse_json_nodes(x, singleNode)
        props = ""
        y = 0 # track how many props are being added
        for key,value in res.iteritems():
            if y > 0: # add comma for every subsequent key/value pair
                props += ',' 
            if isinstance(value, int) or isinstance(value, float):
                props += '`%s`:%s' % (key,value)
                y += 1
            else:
                value = value.replace('"',"'")
                props += '`%s`:"%s"' % (key,value)
                y += 1
        if 'node_type' in res: # if no node type, need to ignore     
            cstr = "CREATE (node:`%s` { %s })" % (nodes[res['node_type']],props) # create should make this faster, trust CouchDB to guarantee unique
            o.write("%s\n" % (cstr.encode('utf-8')))
            if n % 50 == 0: # very likely that for each doc, tens of transactions need to occur. Break up commits here.
                o.write(';\n')
                o.write('COMMIT\n')
                o.write('BEGIN\n')
        if n % 500 == 0:
            print "%s documents converted into nodes and in Neo4j" % (n)
        n += 1

o.write(';\n')
o.write('COMMIT\n')

print "Finished processing a total of %s documents for adding NODES. Now to process edges..." % (n)

#########
# EDGES #
#########

de = set (definitive_edges)
o.write('BEGIN\n')

# Function to build an edge between two nodes. Accepts the following parameters:
# n1: Neo4j node name of the document currently being processed
# id: ID of this n1 source node
# n2: Neo4j node name relating to the node that n1 should be attached to
# n2_pv: The property and value of the second node to match by
# link: Neo4j edge name that is to be used
def build_edge(n1,id,link,n2,n2_p,n2_v):
    cstr = ""
    if n2 != 'noidea': # Both node names are known here
        cstr = "MATCH (n1:`%s`{`id`:'%s'}),(n2:`%s`{`%s`:'%s'}) CREATE (n1)-[:%s]->(n2)" % (n1,id,n2,n2_p,n2_v,link)
    else: # Don't know what the node is definitively, have to find it
        cstr = "MATCH (n1:`%s`{`id`:'%s'}),(n2 {`%s`:'%s'}) CREATE (n1)-[:%s]->(n2)" % (n1,id,n2_p,n2_v,link)
    o.write("%s\n" % (cstr.encode('utf-8')))

# Recurse through JSON object and find anything that pertains to an edge. 
def traverse_json_edges(x, snode):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "" or not v: 
                pass
            elif k == "node_type" or k == "tags" or k == "id":
                if k not in snode:
                    snode[k] = v
            elif k == "linkage":
                for key,value in v.iteritems():
                    snode[k].append([key,value])
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
                                combined = "%s:%s" % (key,z)
                                if combined not in snode[k]:
                                    snode[k].append(combined)
                        else:
                            if isinstance(value, str):
                                value = value.replace("'","\'")
                                value = value.replace('"','\"')
                            combined = "%s:%s" % (key,value)
                            if combined not in snode[k]:
                                snode[k].append(combined)

        return max(traverse_json_edges(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json_edges(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Regex needed to extract prop+value combo from MIMARKS and mixs
regexForPropValue = r"(.*):(.*)"

# Some terminal feedback
print "Beginning to add the edges to all docs within the file '%s'. Total number of docs (including _hist) is = %s" % (sys.argv[1],len(docList))
tot = 0 # count total number of edges for verification
breaks1 = 50 # breakup the Neo4j commit file by rough estimate of edges per document (1 doc ~= 10 edges)
breaks2 = 1000 # break up the terminal output by number of edges added
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize dict at each new document
        singleNode['mimarks'] = []
        singleNode['mixs'] = []
        singleNode['linkage'] = []
        res = traverse_json_edges(x, singleNode)
        id = res['id']
        mm = res['mimarks']
        mx = res['mixs']
        lk = res['linkage'] # two elements = edge type + id (so 1 edge)
        tg = ""
        nt = ""

        if 'node_type' in res: # must be a node if edges are to be added
            nt = res['node_type']
        
        if 'tags' in res: # some nodes may be missing this. 
            tg = res['tags']
            for tag in tg:
                build_edge(nodes[nt],id,edges['has_tag'],nodes['tags'],'term',tag)
        
        for mim in mm:
            p = re.search(regexForPropValue,mim).group(1)
            v = re.search(regexForPropValue,mim).group(2)
            build_edge(nodes[nt],id,edges['has_mimarks'],nodes['mimarks'],p,v)

        for mix in mx:
            p = re.search(regexForPropValue,mix).group(1)
            v = re.search(regexForPropValue,mix).group(2)
            build_edge(nodes[nt],id,edges['has_mixs'],nodes['mixs'],p,v)

        for links in lk:
            if links[0] in definitive_edges:
                build_edge(nodes[nt],id,edges[links[0]],definitive_edges[links[0]],'id',[links[1]][0][0])
            else:
                build_edge(nodes[nt],id,edges[links[0]],'noidea','id',[links[1]][0][0])

        tot += (1+len(tg)+len(mm)+len(mx)+len(lk)/2) 
        if tot > breaks2:
            print "%s\t\tedges added." % (breaks2)

            breaks2 += 1000

        breaks1 += 1
        if tot > breaks1:
            o.write(';\n')
            o.write('COMMIT\n')
            o.write('BEGIN\n')
            breaks1 += 50

o.write(';\n')
o.write('COMMIT\n')

print "Finished. Attached a total of %s edges." % (tot)
print "Now use the file generated by this to run: ~/path/to/neo4j-shell -path ~/data/databases/my_graph.db -c < ~/path/to/this/output/couchdb2neo4j.cypher"
