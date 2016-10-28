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
from dicts_for_flattened_couchdb2neo4j import nodes, edges

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Make a set out of the known edges so we can create them when found. 
e = set(edges)

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

        return max(traverse_json(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Regex needed to extract prop+value combo from MIMARKS and mixs
regexForPropValue = r"(.*):(.*)"

# Some terminal feedback
print "Beginning to add the edges to all docs within the file '%s'. Total number of docs (including _hist) is = %s" % (sys.argv[1],len(docList))
tot = 0 # count total number of edges for verification
breaks = 1000 # break up the output
# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        singleNode = {} # reinitialize dict at each new document
        singleNode['mimarks'] = []
        singleNode['mixs'] = []
        singleNode['linkage'] = []
        res = traverse_json(x, singleNode)
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
            cstr = "MATCH (n1:`%s`{`id`:'%s'}),(n2 {`%s`:'%s'}) CREATE (n1)-[:%s]->(n2)" % (nodes[nt],id,edges[links[0]],'id',[links[1]][0][0])
            cypher.run(cstr)

        tot += (1+len(tg)+len(mm)+len(mx)+len(lk)/2) 
        if tot > breaks:
            print "%s\t\tedges added." % (breaks)
            breaks += 1000

print "Finished. Attached a total of %s edges." % (tot)