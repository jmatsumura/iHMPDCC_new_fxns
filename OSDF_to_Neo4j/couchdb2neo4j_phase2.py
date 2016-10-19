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

# Function to build an index in Neo4j to make edge connection a bit faster during
# the lookup phase. Accepts the name of a node (possible values in dicts_for_couchdb2neo4j)
# and the property that that node ought to be indexed by. 
def build_index(node,prop):
    cstr = "CREATE INDEX ON: %s(%s)" % (node,prop)
    cypher.run(cstr)

# Function to build an edge between two nodes. Accepts the following parameters:
# n1: Neo4j node name of the document currently being processed
# id: ID of this n1 source node
# n2: Neo4j node name relating to the node that n1 should be attached to
# n2_pv: The property and value of the second node to match by
# link: Neo4j edge name that is to be used
def build_edge(n1,id,link,n2,n2_p,n2_v):
    cstr = ""
    if n2 != 'noidea': # Both node names are known here
        cstr = "MERGE (n1:%s{`id`:'%s'})-[:%s]-(n2:%s{`%s`:'%s'})" % (n1,id,link,n2,n2_p,n2_v)
    else: # Don't know what the node is definitively, have to find it
        cstr = "MERGE (n1:%s{`id`:'%s'})-[:%s]-(n2 {`%s`:'%s'})" % (n1,id,link,n2_p,n2_v)
    cypher.run(str)

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

        return max(traverse_json(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Regex needed to extract prop+value combo from MIMARKS and mixs
regexForPropValue = r"(.*):(.*)"

# Some terminal feedback
print "Beginning to add the edges to all docs within the file '%s'. Total number of docs (including _hist) is = %s" % (sys.argv[1],len(docList))
tot = 0 # count total number of edges for verification
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize dict at each new document
        singleNode['mimarks'] = []
        singleNode['mixs'] = []
        singleNode['linkage'] = []
        res = traverse_json(x, singleNode)
        id = res['id']
        nt = res['node_type']
        tg = res['tags']
        mm = res['mimarks']
        mx = res['mixs']
        lk = res['linkage'] # two elements = edge type + id (so 1 edge)
        
        for tag in tg:
            build_edge(nt,id,edges['has_tag'],nodes['tags'],'term',tag)
        
        for mim in mm:
            p = re.search(regexForPropValue,mim).group(1)
            v = re.search(regexForPropValue,mim).group(2)
            build_edge(nt,id,edges['has_mimarks'],nodes['mimarks'],p,v)

        for mix in mx:
            p = re.search(regexForPropValue,mix).group(1)
            v = re.search(regexForPropValue,mix).group(2)
            build_edge(nt,id,edges['has_mixs'],nodes['mixs'],p,v)

        for links in lk:
            if links[0] in definitive_edges:
                build_edge(id,nt,edges[links[0]],definitive_edges[links[0]],'id',[links[1]][0][0])
            else:
                build_edge(id,nt,edges[links[0]],'noidea','id',[links[1]][0][0])

        n = 0 # find how many edges added from this doc
        n += (1+len(tg)+len(mm)+len(mx)+len(lk)/2) 
        tot += n
        print "%s\t\tedges added, totaling to\t\t%s" % (n,tot)

print "Finished. Attached a total of %s edges." % (tot)
