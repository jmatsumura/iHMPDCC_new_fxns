#!/usr/bin/python
#
# Script to go through a CouchDB changes feed of OSDF data to update the 
# respective Neo4j instance. This script is meant to be fairly resilient 
# and should be able to run against any relevant set of the CouchDB feed. 
# It makes sure only the newest version of a node is kept and uses MERGE 
# on node/edge creation so that no duplicates should be present (e.g. 
# you could run this multiple times against the exact same CouchDB feed 
# and it shouldn't change anything in the database after the first 
# iteration). This also means it can be run against a live database as 
# it won't disrupt any aspect of an up-to-date node but it will 
# incorporate any of the changes noted in the feed.
#
# Accepts the following parameter:
# 1) Path to couchdb_changes_feed.json file
#

import json, sys, re, urllib2
from py2neo import Graph
from collections import defaultdict
from accs_for_flattened_couchdb2neo4j import nodes, edges, body_site_dict, fma_free_body_site_dict, mod_quotes, definitive_edges2

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['results']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Function to build an index in Neo4j to make edge connection a bit faster during
# the lookup phase. Accepts the name of a node (possible values in dicts_for_couchdb2neo4j)
# and the property that that node ought to be indexed by. 
def build_constraint_index(node,prop):
    cypher.run("CREATE CONSTRAINT ON (x:%s) ASSERT x.%s IS UNIQUE" % (node,prop))

build_constraint_index('Case','id')
build_constraint_index('File','id')
build_constraint_index('Tags','term')

# Skip any nested dictionaries like those under 'doc' or 'meta'. 'linkage' is
# skipped since this script is only concerned with creating nodes, not edges.
# Also skip numerous CouchDB specific attributes (_rev, rev, key, _id). 
skipUs = ['value','doc','meta','changes','acl','_rev','rev','key','_id','_search']
skip = set(skipUs) 

regex_for_id = r'`id`:"([a-zA-Z0-9]*)"'
regex_for_ver = r'`ver`:(\d+)'

# Recurse through JSON object. Note that throughout this function many nodes are
# likely to be created per document depending on the number of unique tags found.
# Arguments:
# x = JSON object
# snode = Dictionary to pass through to extract all key/value pairs from JSON
def traverse_json(x, snode, id):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "" or not v: # check for empty string/list 
                continue
            elif k in skip: # skip info we don't want to transfer and edge info for now
                continue
            else: 
                # Tags (list), MIMARKS (dict), and mixs (dict), should be individual nodes so add now
                if k == "tags": # new node for each new tag in this list
                    for tag in v:
                        tag = mod_quotes(tag)
                        cypher.run("MERGE (n1:Tags{term:'%s'})" % (tag))
                        cypher.run("MATCH (n1:Tags{term:'%s'}),(n2{id:'%s'}) MERGE (n1)<-[:HAS_TAG]-(n2)" % (tag,id))

                elif k == "mimarks" or k == 'mixs':

                    # Need to establish this connection on the onset of node creation
                    meta_edge = ""
                    if k == "mimarks":
                        meta_edge = "HAS_MIMARKS"
                    else:
                        meta_edge = "HAS_MIXS"

                    for key,value in v.iteritems():
                        if value == "" or not value: # check for empty string/list
                            continue
                        else:
                            if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                                for z in value:
                                    z = mod_quotes(z)
                                    cypher.run("MERGE (n1:%s{%s:'%s'})" % (nodes[k],key,z))
                                    cypher.run("MATCH (n1:%s{%s:'%s'}),(n2{id:'%s'}) MERGE (n1)<-[:%s]-(n2)" % (nodes[k],key,z,id,meta_edge))
                            else:
                                value = mod_quotes(value)
                                cypher.run("MERGE (n1:%s{%s:'%s'})" % (nodes[k],key,value))
                                cypher.run("MATCH (n1:%s{%s:'%s'}),(n2{id:'%s'}) MERGE (n1)<-[:%s]-(n2)" % (nodes[k],key,value,id,meta_edge))

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

        return max(traverse_json(x[a], snode, id) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode, id) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Arguments:
# x = JSON CouchDB doc
# id = node ID
def create_node(x,id):
    singleNode = {} # reinitialize dict at each new document

    # Need to create an essentially blank node at the very least if the ID is
    # present. Note that  two nodes are created here because we do not yet 
    # know whether this is a Case or File node. Create both and then
    # delete the one once we extract and have this info. 
    cypher.run("MERGE (n:Case{`id`:'%s'})" % (id))
    cypher.run("MERGE (n:File{`id`:'%s'})" % (id))

    res = traverse_json(x, singleNode, id)
    fma = False # don't know whether or not it has FMA body site property
    props = ""
    y = 0 # track how many props are being added
    for key,value in res.iteritems():

        if y > 0: # add comma for every subsequent key/value pair
            if props[-1:] != ",": # ensure no comma follows another, can arise from body site skip
                props += ',' 

        if key == 'fma_body_site':
            props += '`%s`:"%s"' % (key,fma_free_body_site_dict[value])
            y += 1
            fma = True
            continue # continue makes sure we don't add more than one fma_body_site property
        elif key == 'body_site':
            if fma == True: # already seen FMA body site, forget body_site
                continue
            else: # need check all other keys to make sure FMA body site isn't in the future
                for key,value in res.iteritems():
                    if key == 'fma_body_site':
                        fma == True
                        break
                if fma == False: # if no FMA present, use body site to map
                    props += '`%s`:"%s"' % ('fma_body_site',fma_free_body_site_dict[value])
                    y += 1
                    continue
                else: # FMA will be found later, use that and skip body_site prop
                    continue

        if isinstance(value, int) or isinstance(value, float):
            props += '`%s`:%s' % (key,value)
            y += 1
        else:
            value = mod_quotes(value)
            props += '`%s`:"%s"' % (key,value)
            y += 1

    if 'node_type' in res: # if no node type, need to ignore   
        # handle the case where the final prop is a body site and an FMA body site
        # already exists so the end of the prop:val string is a trailing comma
        if props[-1:] == ",": 
            props = props[:-1]

        # Identify whether this is a Case or File node using the mapping from
        # accs_for_flattened_couchdb2neo4j
        case_or_file = nodes[res['node_type']]

        # Delete the irrelevant node type. When iterating over all the properties
        # if any metadata is present (tags/mixs/MIMARKS) then these relationships
        # were created. Thus, need to make sure to use DETACH DELETE. 
        if case_or_file != 'Case':
            cypher.run("MATCH (n:Case{`id`:'%s'}) DETACH DELETE n" % (id))
        elif case_or_file != 'File':
            cypher.run("MATCH (n:File{`id`:'%s'}) DETACH DELETE n" % (id))

        cypher.run("MATCH (n:%s{`id`:'%s'}) SET n = { %s }" % (case_or_file,res['id'],props))
        print(("MATCH (n:%s{`id`:'%s'}) SET n = { %s }" % (case_or_file,res['id'],props)).encode('utf-8'))

print "CouchDB feed imported. Now inserting/deleting nodes..."

# Iterate over each doc from CouchDB and insert the nodes into Neo4j. While this
# happens, note which edges need to be created and add them in after. 
edge_dict = defaultdict(list)
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None and re.match(r'\_design.*', x['id']) is None: # ignore history/design documents
        if 'deleted' in x:
            if x['deleted'] == True:
                cypher.run("MATCH (n{`id`:'%s'}) DETACH DELETE n" % (x['id']))
                continue # delete and move on

        # Only valid nodes have a version
        if 'ver' in x['doc']:
            # Grab the current live version and check if it needs an update
            cquery = "MATCH ((n{`id`:'%s'})) RETURN n" % (x['id'])
            node = graph.data(cquery)

            # Node already is in the database
            if node:
                node = node[0] # subset since we know it is just one node
                node = node['n']

                # If the node has a version already, make sure only the newest is kept
                if 'ver' in node:
                    # Update if this is a newer version
                    if int(x['doc']['ver']) > int(node['ver']):
                        
                        # Drop the old tags/metadata connections as well as where
                        # this node points to upstream. Note that this will not Drop
                        # any relationship going to this node as we cannot know if
                        # that has been changed. 
                        cypher.run("MATCH (n{`id`:'%s'})-[r]->(x) DELETE r" % (id))

                        # Drop all properties EXCEPT for ID from this node. 
                        # This helps maintain old edge connections while still
                        # allowing only the new properties to be added to the
                        # node. 
                        for prop in node:
                            if prop != 'id':
                                cypher.run("MATCH (n{`id`:'%s'}) REMOVE n.%s" % (id,prop))

                        create_node(x,x['id'])

                        # Now that the node is updated, make sure it has the correct edges
                        if 'linkage' in x['doc']:
                            for edge in edges:
                                if edge in x['doc']['linkage']:
                                    for upstream in x['doc']['linkage'][edge]:
                                        # Capture the ID:ID values that this edge is between.
                                        relationship = "%s:%s" % (x['id'],upstream)
                                        edge_dict[edges[edge]].append(relationship)

            # Node doesn't exist yet, place in DB
            else:
                create_node(x,x['id'])
                if 'linkage' in x['doc']: # add edge for this new node if it is known
                    for edge in edges:
                        if edge in x['doc']['linkage']:
                            for upstream in x['doc']['linkage'][edge]:
                                # Capture the ID:ID values that this edge is between.
                                relationship = "%s:%s" % (x['id'],upstream)
                                edge_dict[edges[edge]].append(relationship)

print "Node creation/deletion complete, adding edges..."

# All the nodes have been created, add the edges now. Doing it in this order 
# bypasses any issue of node creation order in the feed so it will readily handle
# when a downstream node is inserted before an upstream node. 
for edge,link_us in edge_dict.items():

    # Iterate over all edges outgoing from this particular node. In most cases
    # this will just be one relationship but cases like the 'omes have multiple.
    for nodes in link_us:
        vals = nodes.split(':')
        n1 = vals[0]
        n2 = vals[1]
        cypher.run("MATCH (n1{`id`:'%s'}),(n2{`id`:'%s'}) MERGE (n1)-[:%s]->(n2)" % (n1,n2,edge))
        print("MATCH (n1{`id`:'%s'}),(n2{`id`:'%s'}) MERGE (n1)-[:%s]->(n2)" % (n1,n2,edge))

print "Now purging test/redundant/irrelevant data (see comments in code for specifics)..."
# Removing test data based on those linked to the 'Test Project' node.
cypher.run("MATCH (P:Case{node_type:'project'})<-[*..20]-(n) WHERE P.project_name='test' DETACH DELETE n,P")
cypher.run("MATCH (P:File{node_type:'16s_dna_prep'})<-[*..20]-(n) WHERE P.project_name='blah' DETACH DELETE n,P")
# Removing the demo HMP study as this is redundant and all downstream files accounted for by individual studies.
cypher.run("MATCH (S:Case{node_type:'study'}) WHERE S.name='Human microbiome project demonstration projects.' DETACH DELETE S")
# Removing additional test node artifacts from OSDF.
cypher.run("MATCH (n:Case{node_type:'sample'}) WHERE n.fma_body_site='test' DETACH DELETE n")
cypher.run("MATCH (n{id:'610a4911a5ca67de12cdc1e4b40135fe'}) DETACH DELETE n")
cypher.run("MATCH (n{id:'3fffbefb34d749c629dc9d147b238f67'}) DETACH DELETE n")
# Removing any nodes which have no relationships, should not ever be the case.
cypher.run("MATCH (n) WHERE size((n)--())=0 DELETE n")

print "Neo4j database now up-to-date with the given CouchDB changes feed."
        