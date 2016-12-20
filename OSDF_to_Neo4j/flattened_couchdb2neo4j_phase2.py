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
from accs_for_flattened_couchdb2neo4j import nodes, edges, definitive_edges, mod_quotes

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

def build_edge(n1,id,link,n2,n2_p,n2_v):
    cstr = "MATCH (n1:`%s`{`id`:'%s'}),(n2:`%s`{`%s`:'%s'}) CREATE (n1)-[:%s]->(n2)" % (n1,id,n2,n2_p,n2_v,link)
    cypher.run(cstr)

# Make a set out of the known edges so we can create them when found. 
e = set(edges)

# Recurse through JSON object and find anything that pertains to an edge. 
def traverse_json(x, snode):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "" or not v: 
                continue
            elif k == "node_type" or k == "tags" or k == "id":
                if k not in snode:
                    snode[k] = v
            elif k == "linkage":
                for key,value in v.iteritems():
                    snode[k].append([key,value])
            elif k == "mimarks" or k == 'mixs':
                for key,value in v.iteritems():
                    if value == "" or not value: # check for empty string/list
                        continue
                    else:
                        if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                            for z in value:
                                if isinstance(z, str):
                                    z = z.replace("'","\'")
                                    z = z.replace('"','\"')
                                combined = "%s`````%s" % (key,z) # use this marking to make separation easy
                                if combined not in snode[k]:
                                    snode[k].append(combined)
                        else:
                            if isinstance(value, str):
                                value = value.replace("'","\'")
                                value = value.replace('"','\"')
                            combined = "%s`````%s" % (key,value)
                            if combined not in snode[k]:
                                snode[k].append(combined)

        return max(traverse_json(x[a], snode) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Regex needed to extract prop+value combo from MIMARKS and mixs
regexForPropValue = r"(.*)`````(.*)"

# Some terminal feedback
print "Beginning to add the edges to all docs within the file '%s'. Total number of docs (including _hist) is = %s" % (sys.argv[1],len(docList))
tot = 0 # count total number of edges for verification
breaks = 5000 # break up the output
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
                tag = mod_quotes(tag)
                build_edge(nodes[nt],id,edges['has_tag'],nodes['tags'],'term',tag)
                tot += 1
        
        for mim in mm:
            p = re.search(regexForPropValue,mim).group(1)
            v = re.search(regexForPropValue,mim).group(2)
            v = mod_quotes(v)
            build_edge(nodes[nt],id,edges['has_mimarks'],nodes['mimarks'],p,v)
            tot += 1

        for mix in mx:
            p = re.search(regexForPropValue,mix).group(1)
            v = re.search(regexForPropValue,mix).group(2)
            v = mod_quotes(v)
            build_edge(nodes[nt],id,edges['has_mixs'],nodes['mixs'],p,v)
            tot += 1

        for links in lk:
            for x in links[1]:
                x = mod_quotes(x)
                if links[0] in definitive_edges:
                    build_edge(nodes[nt],id,edges[links[0]],definitive_edges[links[0]],'id',x)
                else: # know that we aren't dealing with case or other labels
                    build_edge(nodes[nt],id,edges[links[0]],'File','id',x) 
                tot += 1

        if tot > breaks:
            print "%s\t\tedges added." % (breaks)
            breaks += 5000

print "Finished phase 2. Attached a total of %s edges." % (tot)

print "Now removing test data based on those linked to the 'Test Project' node..."
cypher.run("MATCH (P:Case{node_type:'project'})<-[*..20]-(n) WHERE P.project_name='test' DETACH DELETE n,P")
cypher.run("MATCH (P:File{node_type:'16s_dna_prep'})<-[*..20]-(n) WHERE P.project_name='blah' DETACH DELETE n,P")

print "Now removing the demo HMP study as this is redundant and all downstream files accounted for by individual studies..."
cypher.run("MATCH (S:Case{node_type:'study'}) WHERE S.name='Human microbiome project demonstration projects.' DETACH DELETE S")

print "Now removing additional test node artifacts from OSDF..."
cypher.run("MATCH (n:Case{node_type:'sample'}) WHERE n.fma_body_site='test' DETACH DELETE n")
cypher.run("MATCH (n{id:'610a4911a5ca67de12cdc1e4b40135fe'}) DETACH DELETE n")
cypher.run("MATCH (n{id:'3fffbefb34d749c629dc9d147b238f67'}) DETACH DELETE n")

print "All done!"
