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
def traverse_json(x, arr, uset):
    if type(x) is dict and x: # iterate over each dictionary

        for k,v in x.iteritems():
            if v == "": 
                pass
            elif k in skip or k in e: # skip info we don't want to transfer and edge info for now
                pass
            else: 
                if k == "tags": # new node for each new tag
                    for j in v:
                        if j not in uset and len(j)<25:
                            uset.add(j) # union to add values
                            cstr = "CREATE (node:Tags{term:'%s'})" % (j)
                            cypher.run(cstr)

        return max(traverse_json(x[a], arr, uset) for a in x)

    if type(x) is list and x: # handle potential lists of dictionaries
        return max(traverse_json(a, arr, uset) for a in x)
        
    return arr

uniqueTags = set() # Create one 'Tag' node per unique found tag property value

for x in docList:
    if re.match(r'\w+\_hist', x['id']) is None: # ignore history documents
        arr = [] # reinitialize array at each new document
        traverse_json(x, arr, uniqueTags)
        print (len(uniqueTags))
