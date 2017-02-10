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
# This script is broken into two sections: FUNCTIONS and DB CREATION
#
# Accepts the following parameter:
#
# Path to conf file with content in the following format (one on each line):
# COUCHDB_HOST=http://osdf-ihmp.igs.umaryland.edu
# COUCHDB_PORT=5984
# COUCHDB_OSDF_DB=OSDF
# CHANGES_SINCE=1
# NEO4J_PASS=example
# 
# 
# If you this is the first run, make sure CHANGES_SINCE=0.
# Note that this script will overwrite the value of CHANGES_SINCE in this file
# so that it can use the same one to determine where it needs to pick up.

import json, sys, re, urllib2
from py2neo import Graph
from collections import defaultdict
from accs_for_couchdb2neo4j import nodes, edges, body_site_dict, fma_free_body_site_dict, mod_quotes, definitive_edges2
from accs_for_couchdb2neo4j import body_product_dict, study_name_dict, file_format_dict

#############
# FUNCTIONS #
#############

# Function to build an index in Neo4j to make edge connection a bit faster during
# the lookup phase. Accepts the name of a node (possible values in dicts_for_couchdb2neo4j)
# and the property that that node ought to be indexed by. 
def build_constraint_index(node,prop):
    cypher.run("CREATE CONSTRAINT ON (x:%s) ASSERT x.%s IS UNIQUE" % (node,prop))

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

# Function to create or revise a node. 
# Arguments:
# x = JSON CouchDB doc
# id = node ID
def create_node(x,id):

    single_node = {} # reinitialize dict at each new document

    # Need to create an essentially blank node at the very least if the ID is
    # present. Note that  two nodes are created here because we do not yet 
    # know whether this is a Case or File node. Create both and then
    # delete the one once we extract and have this info. 
    cypher.run("MERGE (n:Case{`id`:'%s'})" % (id))
    cypher.run("MERGE (n:File{`id`:'%s'})" % (id))

    res = traverse_json(x, single_node, id)
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

    # If no node type, something has gone awry with an earlier check either here
    # or OSDF so should ignore   
    if 'node_type' in res: 

        # handle the case where the final prop is a body site and an FMA body site
        # already exists so the end of the prop:val string is a trailing comma
        if props[-1:] == ",": 
            props = props[:-1]

        # If a version was never established, include one now so that this
        # can be easily compared with future iterations of the node in this
        # changes feed loader. 
        if 'ver' not in res:
            props += ',`ver`:1'

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

# Function to build an edge dictionary that will build all edges once all
# new changes are up-to-date. A dictionary needs to be built here because,
# unlike CouchDB, in the Neo4j version of the DB we need to ensure all
# nodes are already present before adding edges. 
# Arguments:
# edge_dict = a defaultdict(list) that will denote all the edges, other than 
# those tied to metadata, that need to be added.
# doc = document from CouchDB that has edges which need to be added
def create_edge(edge_dict,doc):

    if 'linkage' in doc['doc']:
        # Cover all valid edges tied to the linkage key
        for edge in edges:
            if edge in doc['doc']['linkage']:
                # Occasionally there are multiple nodes to be linked to given
                # a single edge type ('~omes'). Distinguish between that and
                # a single value.
                if type(doc['doc']['linkage'][edge]) is list:
                    for upstream in doc['doc']['linkage'][edge]:
                        # Capture the ID:ID values that this edge is between.
                        relationship = "%s:%s" % (doc['id'],upstream)
                        edge_dict[edges[edge]].append(relationship)
                # The more common case, just one node
                else:
                    relationship = "%s:%s" % (doc['id'],doc['doc']['linkage'][edge])
                    edge_dict[edges[edge]].append(relationship)

    return edge_dict

###############
# DB CREATION #
###############

conf_dict = {}

# Parse through the input conf file and extract
with open(sys.argv[1],'r') as conf:
    for line in conf:
        line = line.rstrip()
        ele = line.split('=')
        conf_dict[ele[0]] = ele[1]

host = conf_dict["COUCHDB_HOST"]
port = conf_dict["COUCHDB_PORT"]
osdf = conf_dict["COUCHDB_OSDF_DB"]
since = conf_dict["CHANGES_SINCE"]
neo4jpass = conf_dict["NEO4J_PASS"]

# Establish connection to Neo4j
graph = Graph(password = neo4jpass)
cypher = graph

# Identify the beginning and end document numbers designated by the value of
# the 'seq' key in CouchDB.
beg_doc = "%s:%s/%s/_changes?since=1&limit=1" % (host,port,osdf)
beg_num = json.load(urllib2.urlopen(beg_doc))['last_seq']
end_doc = "%s:%s/%s/_changes?descending=true&limit=1" % (host,port,osdf)
end_num = json.load(urllib2.urlopen(end_doc))['last_seq']

# If we are already up-to-date, leave.
if int(since) == int(end_num):
    exit(0)

# If we are starting at or before the first document, the DB should be blank.
# Thus, build indices.
if int(since) <= int(beg_num):
    build_constraint_index('Case','id')
    build_constraint_index('File','id')
    build_constraint_index('Tags','term')

# Skip any nested dictionaries like those under 'doc' or 'meta'. 'linkage' is
# skipped since this script is only concerned with creating nodes, not edges.
# Also skip numerous CouchDB specific attributes (_rev, rev, key, _id). 
skipUs = ['value','doc','meta','changes','acl','_rev','rev','key','_id','_search','seq']
skip = set(skipUs) 

# Regex to skip irrelevant documents for Neo4j's purposes.
regex_for_id = r'`id`:"([a-zA-Z0-9]*)"'
regex_for_ver = r'`ver`:(\d+)'

url = "%s:%s/%s/_changes?since=%s&include_docs=true" % (host,port,osdf,since)
doc_list = json.load(urllib2.urlopen(url))['results']

print "Feed obtained."

# Iterate over each doc from CouchDB and insert the nodes into Neo4j. While this
# happens, note which edges need to be created and add them in after. 
edge_dict = defaultdict(list)
for x in doc_list:

    if re.match(r'\w+\_hist', x['id']) is None and re.match(r'\_design.*', x['id']) is None: # ignore history/design documents
        if 'deleted' in x:
            if x['deleted'] == True:
                cypher.run("MATCH (n{`id`:'%s'}) DETACH DELETE n" % (x['id']))
                continue # delete and move on to next document

        # If node has 'ver' we know that it is likely already in the database
        # since version is explicitly added only when a revision occurs 
        # (although users can manually enter version 1)
        if 'ver' in x['doc']:

            # Grab the current live version and check if it needs an update
            cquery = "MATCH ((n{`id`:'%s'})) RETURN n" % (x['id'])
            node = graph.data(cquery)

            # If someone manually entered version 1, or this is the first time 
            # loading the changes feed, then this check will fail.
            if node:
                node = node[0] # subset since we know it is just one node
                node = node['n']

                # If the node is already in the database, then we need to compare
                # versions and make sure that only the latest data is kept.
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
                    edge_dict = create_edge(edge_dict,x)

            # Need to make sure nodes that are designated as version 1 are
            # indeed added as well as the initial loading of any nodes
            # with a version greater than 1. 
            else:
                create_node(x,x['id'])
                edge_dict = create_edge(edge_dict,x)

        # Node is a first iteration, hasn't been placed in the DB yet, and lacks
        # an explicit version 1.
        else:
            create_node(x,x['id'])
            edge_dict = create_edge(edge_dict,x)

# All the nodes have been created, add the edges now. Doing it in this order 
# bypasses any issue of node creation order in the feed so it will readily handle
# when a downstream node is inserted before an upstream node. Before the edges
# are actually added, we want to make sure our changes feed caught a batch of 
# changes that contain all the information to build a new set. Meaning, we want 
# to ensure that for all the edges to add the upstream node has already been
# created (don't want to catch the middle of a batch insertion).
nodes_from_edges,all_nodes = (set() for i in range(2))
for edge,link_us in edge_dict.items():
    # Iterate over all edges outgoing from this particular node. In most cases
    # this will just be one relationship but cases like the 'omes have multiple.
    for nodes in link_us:
        vals = nodes.split(':')
        nodes_from_edges.add(vals[0])
        nodes_from_edges.add(vals[1])

# Extract all the nodes from Neo4j
cquery = "MATCH (n) WHERE exists(n.id) RETURN n.id AS id"
nodes = graph.data(cquery)
for x in range(0,len(nodes)):
    all_nodes.add(nodes[x]['id'])

# If we find that all nodes for this particular set of edges are not present,
# then simply leave early. This will leave the conf file where it was so that 
# it picks up here again and eventually it will capture a timepoint with a
# complete picture. 
if not nodes_from_edges.issubset(all_nodes):
    exit(0)

for edge,link_us in edge_dict.items():
    for nodes in link_us:
        vals = nodes.split(':')
        n1 = vals[0]
        n2 = vals[1]
        cypher.run("MATCH (n1{`id`:'%s'}),(n2{`id`:'%s'}) MERGE (n1)-[:%s]->(n2)" % (n1,n2,edge))

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
# This will catch those metadata nodes which are now stragglers due to a 
# particular case/file node associated with them being deleted. 
cypher.run("MATCH (n) WHERE size((n)--())=0 DELETE n")
# Renaming some names for the project and study nodes. 
cypher.run("MATCH (n) WHERE n.node_type='project' AND n.name='iHMP' SET n.name=n.project_name")
# Still need to switch acronyms here for studies once they're approved.

# Finally, overwrite the initial conf to reflect the last document updated so
# that this script will resume from that point next time. 
with open(sys.argv[1],'w') as conf:
    conf.write("COUCHDB_HOST=%s\n" % host)
    conf.write("COUCHDB_PORT=%s\n" % port)
    conf.write("COUCHDB_OSDF_DB=%s\n" % osdf)
    conf.write("CHANGES_SINCE=%s\n" % end_num)
    conf.write("NEO4J_PASS=%s\n" % neo4jpass)
    