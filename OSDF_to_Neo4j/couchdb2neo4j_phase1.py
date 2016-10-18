#!/usr/bin/python
#
# Script to go through a CouchDB dump of OSDF data and create the respective
# nodes in Neo4j. Requires the output of get_indices.py for faster processing
# so that it has indices of related notes within the file.
#
# Phase 1 consists of simply converting all documents+attributes into
# nodes+properties. Phase 2 will add the relevant edges to the nodes. 

import json, sys, re
from py2neo import Graph

i = open(sys.argv[1], 'r') # couchdb dump json is the input
json_data = json.load(i) 
docList = json_data['rows']

neo4j_password = "neo4j" # Neo4j setup
graph = Graph(password = neo4j_password)
cypher = graph

# Below are dictionaries to convert from OSDF syntax to what will be loaded
# in Neo4j. 
nodes = {
    'project': 'Project',
    'study': 'Study',
    'subject': 'Subject',
    'subject_attr': 'Subject_Attr',
    'visit': 'Visit',
    'visit_attr': 'Visit_Attr',
    'sample': 'Sample',
    'sample_attr': 'Sample_Attr',
    'wgs_dna_prep': 'WGS_DNA_Prep',
    'host_seq_prep': 'Host_Seq_Prep',
    'wgs_raw_seq_set': 'WGS_Raw_Seq_Set',
    'host_wgs_raw_seq_set': 'Host_WGS_Raw_Seq_Set',
    'microb_transcriptomics_raw_seq_set': 'Microb_Transcriptomics_Raw_Seq_Set',
    'host_transcriptomics_raw_seq_set': 'Host_Transcriptomics_Raw_Seq_Set',
    'wgs_assembled_seq_set': 'WGS_Assembled_Seq_Set',
    'viral_seq_set': 'Viral_Seq_Set',
    'annotation': 'Annotation',
    'clustered_seq_set': 'Clustered_Seq_set',
    '16s_prep': '16S_Prep',
    '16s_raw_seq_set': '16S_Raw_Seq_Set',
    '16s_trimmed_seq_set': '16S_Trimmed_Seq_Set',
    'microb_assay_prep': 'Microb_Assay_Prep',
    'host_assay_prep': 'Host_Assay_Prep',
    'proteome': 'Proteome',
    'metabolome': 'Metabolome',
    'lipidome': 'Lipidome',
    'cytokine': 'Cytokine',
    'abundance_matrix': 'Abundance_Matrix',
    'tags': 'Tags',
    'mimarks': 'MIMARKS',
    'mixs': 'Mixs'
}

edges = {
    'part_of': 'PART_OF',
    'participates_in': 'PARTICIPATES_IN',
    'associated_with': 'ASSOCIATED_WITH',
    'by': 'BY',
    'collected_during': 'COLLECTED_DURING',
    'prepared_from': 'PREPARED_FROM',
    'sequenced_from': 'SEQUENCED_FROM',
    'derived_from': 'DERIVED_FROM',
    'computed_from': 'COMPUTED_FROM'
}

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
            if v == "": 
                pass
            elif k in skip or k in e: # skip info we don't want to transfer and edge info for now
                pass
            else: 
                # Tags (list), MIMARKS (dict), and mixs (dict), should be individual nodes so add now
                if k == "tags": # new node for each new tag in this list
                    for tag in v:
                        cstr = "MERGE (node:Tags { term:'%s' })" % (tag)
                        cypher.run(cstr)
                elif k == "mimarks" or k == 'mixs':
                    for key,value in v.iteritems():
                        if value == "" or not value: # check for empty string/list
                            pass
                        else:
                            if isinstance(value, list): # some of the values in mixs/MIMARKS are lists
                                for z in value:
                                    cstr = "MERGE (node:%s { %s:'%s' })" % (nodes[k],key,z)
                                    cypher.run(cstr)
                            else:
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
                    
                    if k == "urls":
                        prop = "" # variable prop name for each URL, shouldn't expect consistent ordering
                        for file in v:
                            if 'ftp://' in file:
                                prop = "ftp"
                            elif 'http://' in file:
                                prop = "http"
                            elif 's3://' in file:
                                prop = "s3"
                            if prop not in snode: # ensure no dupes due to recursion
                                snode[prop] = file

                    else: 
                        if k not in snode: 
                            snode[k] = v

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
        cypher.run(cstr)
        if m % 500 == 0:
            print "%s documents converted into nodes and in Neo4j" % (m)
        m += 1

print "Finished. Processed a total of %s documents." % (m)
