#!/usr/bin/python
#
# Script to go through a CouchDB changes feed of OSDF data update the respective
# nodes in Neo4j.
#
# Accepts the following parameter:
# 1) Path to couchdb_changes_feed.json file
#

import json, sys, re, urllib2
from py2neo import Graph
from accs_for_flattened_couchdb2neo4j import nodes, edges, body_site_dict, mod_quotes, traverse_json

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['results']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

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
            else: 
                # Tags (list), MIMARKS (dict), and mixs (dict), should be individual nodes so add now
                if k == "tags": # new node for each new tag in this list
                    for tag in v:
                        tag = mod_quotes(tag)
                        cstr = "MERGE (node:Tags{term:'%s'})<-[:HAS_TAG]-(n{id:'%s'})" % (tag,id)
                        #cypher.run(cstr)

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
                                    cstr = "MERGE (node:%s{%s:'%s'})<-[:%s]-(n{id:'%s'})" % (nodes[k],key,z,meta_edge,id)
                                    #cypher.run(cstr)
                            else:
                                value = mod_quotes(value)
                                cstr = "MERGE (node:%s{%s:'%s'})<-[:%s]-(n{id:'%s'})" % (nodes[k],key,value,meta_edge,id)
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

        return max(traverse_json(x[a], snode, id) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, snode, id) for a in x)
        
    return snode # give back the attributes of a single doc which will convert to a single node

# Arguments:
# x = JSON CouchDB doc
# c_or_s = CREATE or SET these values? 
# id = node ID
def create_node(x,c_or_s,id):
    singleNode = {} # reinitialize dict at each new document
    res = traverse_json(x, singleNode, id)
    fma = False # don't know whether or not it has FMA body site property
    props = ""
    y = 0 # track how many props are being added
    for key,value in res.iteritems():

        if y > 0: # add comma for every subsequent key/value pair
            if props[-1:] != ",": # ensure no comma follows another, can arise from body site skip
                props += ',' 

        if key == 'fma_body_site':
            props += '`%s`:"%s"' % (key,body_site_dict[value])
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
                    props += '`%s`:"%s"' % ('fma_body_site',body_site_dict[value])
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

        if c_or_s == 'c':
            cstr = "CREATE (node:`%s` { %s })" % (nodes[res['node_type']],props) 
        else: # need to update/set, not create_node
            cstr = "MATCH (n{`id`:'%s'}) SET n = { %s }" % (res['id'],props) 

        cypher.run(cstr)

# Iterate over each doc from CouchDB and insert the nodes into Neo4j.
for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None and re.match(r'\_design.*', x['id']) is None: # ignore history/design documents
        if 'deleted' in x:
            if x['deleted'] == True:
                cstr = "MATCH (n{`id`:'%s'}) DETACH DELETE n" % (x['id'])
                cypher.run(cstr)
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

                # Only valid nodes have a version
                if 'ver' in node:
                    # Update if this is a newer version
                    if int(x['doc']['ver']) > int(node['ver']):
                        
                        # Drop the old metadata connections, make new ones 
                        # later while extracting the new properties from the 
                        # newer version of this node. 
                        cypher.run("MATCH (n{`id`:'%s'})-[r:HAS_TAG]->(x) DELETE r")
                        cypher.run("MATCH (n{`id`:'%s'})-[r:HAS_MIXS]->(x) DELETE r")
                        cypher.run("MATCH (n{`id`:'%s'})-[r:HAS_MIMARKS]->(x) DELETE r")

                        create_node(x,'s',x['id'])

                        # Now that the node is updated, make sure it has the correct edges
                        if 'linkage' in x['doc']:
                            for edge in edges:
                                if edge in x['doc']['linkage']:
                                    # Using "CREATE UNIQUE" will guarantee that
                                    # the edge is only created if it is missing.
                                    cstr = "MATCH (n1{`id`:'%s'}),(n2{`id`:'%s'}) MERGE (n1)-[:%s]->(n2)" % (x['id'],x['doc']['linkage'][edge][0],edges[edge])
                                    cypher.run(cstr)

            # Node doesn't exist yet, place in DB
            else:
                create_node(x,'c',x['id'])
                if 'linkage' in x['doc']: # add edge for this new node if it is known
                    for edge in edges:
                        if edge in x['doc']['linkage']:
                            cstr = "MATCH (n1{`id`:'%s'}),(n2{`id`:'%s'}) MERGE (n1)-[:%s]->(n2)" % (x['id'],x['doc']['linkage'][edge][0],edges[edge])
                            cypher.run(cstr)
               