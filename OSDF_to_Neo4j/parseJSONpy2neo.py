#!/usr/bin/python
#
# Script to go through a CouchDB dump of OSDF data and create the respective
# nodes in Neo4j. Requires the output of get_indices.py for faster processing
# so that it has indices of related notes within the file. 
#
# Author - Justin Wagner (jwagner@cs.umd.edu)
# Updated by: James Matsumura (jmatsumura@som.umaryland.edu)

import json
import pprint
from py2neo import Graph
neo4j_password = "neo4j"

graph = Graph(password = neo4j_password)
cypher = graph

# Open OSDF json dump
osdf_json_path = 'neo4jdata/couchdb_dump.json'
f = open(osdf_json_path)
json_obj = json.load(f)
docList = json_obj['docs']

# Open lists of Project, Study, Subject, Visit, Sample nodes in OSDF json dump
projectList_path = 'neo4jdata/projectList.txt'
projectFile = open(projectList_path)
projectLine = projectFile.read()
projectIndices = projectLine.rstrip('\n').split(' , ')

studyList_path = 'neo4jdata/studyList.txt'
studyFile = open(studyList_path)
studyLine = studyFile.read()
studyIndices = studyLine.rstrip('\n').split(' , ')

subjectList_path = 'neo4jdata/subjectList.txt'
subjectFile = open(subjectList_path)
subjectLine = subjectFile.read()
subjectIndices = subjectLine.rstrip('\n').split(' , ')

visitList_path = 'neo4jdata/visitList.txt'
visitFile = open(visitList_path)
visitLine = visitFile.read()
visitIndices = visitLine.rstrip('\n').split(' , ')

sampleList_path = 'neo4jdata/sampleList.txt'
sampleFile = open(sampleList_path)
sampleLine = sampleFile.read()
sampleIndices = sampleLine.rstrip('\n').split(' , ')

dnaprep16sList_path = 'neo4jdata/16SDNAprepList.txt'
dnaprep16sFile = open(dnaprep16sList_path)
dnaprep16sLine = dnaprep16sFile.read()
dnaprep16sIndices = dnaprep16sLine.rstrip('\n').split(' , ')

rawseqset16sList_path = 'neo4jdata/16SrawseqsetList.txt'
rawseqset16sFile = open(rawseqset16sList_path)
rawseqset16sLine = rawseqset16sFile.read()
rawseqset16sIndices = rawseqset16sLine.rstrip('\n').split(' , ')

trimmedseqset16sList_path = 'neo4jdata/16StrimmedseqsetList.txt'
trimmedseqset16sFile = open(trimmedseqset16sList_path)
trimmedseqset16sLine = trimmedseqset16sFile.read()
trimmedseqset16sIndices = trimmedseqset16sLine.rstrip('\n').split(' , ')

# Create Project nodes
k = 0
for j in projectIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":Project {"
    count = 0
    print(doc)
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'linkage' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if count > 0:
            createStr += ", "
        createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + (str(doc['meta'][key]).replace("\'", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    print(createStr)
    cypher.run(createStr)
    k = k + 1
print("Created projects " + str(k))

# Create Study nodes and then search for Project nodes that match the linkage of each Study node to create Relationship
k = 0
for j in studyIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":Study {"
    count = 0
    print(doc)
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
                createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'part_of' in doc['linkage'].keys():
        for i in range(0, len(projectIndices)):
             if doc['linkage']['part_of'][0] == docList[int(projectIndices[i])]['_id']:
                 relationshipStr = "MATCH (p:Project {id: '" + docList[int(projectIndices[i])]['_id'] + "'}), (s:Study {_id:'" + doc['_id'] + "'}) CREATE (s)-[:PART_OF]->(p)"
                 cypher.run(relationshipStr)
                 break
    elif 'subset_of' in doc['linkage'].keys():
        for i in range(0, len(studyIndices)):
             if doc['linkage']['subset_of'][0] == docList[int(studyIndices[i])]['_id']:
                 relationshipStr = "MATCH (s1:Study {id: '" + docList[int(studyIndices[i])]['_id'] + "'}), (s2:Study {_id:'" + doc['_id'] + "'}) CREATE (s2)-[:SUBSET_OF]->(s1)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created studies " + str(k))

# Create Subject nodes and then search for Study nodes that match the linkage of each Subject node to create Relationship
k = 0
for j in subjectIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":Subject {"
    count = 0
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
                createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'participates_in' in doc['linkage'].keys():
        for i in range(0, len(studyIndices)):
             if doc['linkage']['participates_in'][0] == docList[int(studyIndices[i])]['_id']:
                 relationshipStr = "MATCH (st:Study {_id: '" + docList[int(studyIndices[i])]['_id'] + "'}), (su:Subject {_id:'" + doc['_id'] + "'}) CREATE (su)-[:PARTICIPATES_IN]->(st)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created subjects " + str(k))

# Create Visit nodes and then search for Subject nodes that match the linkage of each Visit node to create Relationship
k = 0
for j in visitIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":Visit {"
    count = 0
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
                createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'by' in doc['linkage'].keys():
        for i in range(0, len(subjectIndices)):
             if doc['linkage']['by'][0] == docList[int(subjectIndices[i])]['_id']:
                 relationshipStr = "MATCH (su:Subject {_id: '" + docList[int(subjectIndices[i])]['_id'] + "'}), (vi:Visit {_id:'" + doc['_id'] + "'}) CREATE (vi)-[:BY]->(su)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created visits " + str(k))

# Create Sample nodes and then search for Visit nodes that match the linkage of each Sample node to create Relationship
k = 0
for j in sampleIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":Sample {"
    count = 0
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
               createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'collected_during' in doc['linkage'].keys():
        for i in range(0, len(visitIndices)):
             if doc['linkage']['collected_during'][0] == docList[int(visitIndices[i])]['_id']:
                 relationshipStr = "MATCH (vi:Visit {_id: '" + docList[int(visitIndices[i])]['_id'] + "'}), (sa:Sample {_id:'" + doc['_id'] + "'}) CREATE (sa)-[:COLLECTED_DURING]->(vi)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created samples " + str(k))

# Create DNAPrep16s nodes and then search for Sample nodes that match the linkage of each DNAPrep16s node to create Relationship
k = 0
for j in dnaprep16sIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":DNAPrep16s {"
    count = 0
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
                createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'prepared_from' in doc['linkage'].keys():
        for i in range(0, len(sampleIndices)):
             if doc['linkage']['prepared_from'][0] == docList[int(sampleIndices[i])]['_id']:
                 relationshipStr = "MATCH (sa:Sample {_id: '" + docList[int(sampleIndices[i])]['_id'] + "'}), (dnap:DNAPrep16s {_id:'" + doc['_id'] + "'}) CREATE (dnap)-[:PREPARED_FROM]->(sa)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created dnaprep16s " + str(k))

# Create RawSeqSet16s nodes and then search for DNAPrep16s nodes that match the linkage of each RawSeqSet16s node to create Relationship
k = 0
for j in rawseqset16sIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":RawSeqSet16s {"
    count = 0
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
                createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'sequenced_from' in doc['linkage'].keys():
        for i in range(0, len(dnaprep16sIndices)):
             if doc['linkage']['sequenced_from'][0] == docList[int(dnaprep16sIndices[i])]['_id']:
                 relationshipStr = "MATCH (dnap:DNAPrep16s {_id: '" + docList[int(dnaprep16sIndices[i])]['_id'] + "'}), (ra:RawSeqSet16s {_id:'" + doc['_id'] + "'}) CREATE (ra)-[:SEQUENCED_FROM]->(dnap)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created rawseqset16s " + str(k))

# Create TrimmedSeqSet16s nodes and then search for RawSeqSet16s nodes that match the linkage of each TrimmedSeqSet16s node to create Relationship
k = 0
for j in trimmedseqset16sIndices:
    doc = docList[int(j)]
    createStr = "CREATE (node" + str(k) + ":TrimmedSeqSet16s {"
    count = 0
    for key in doc.keys():
        if key == 'meta' or key == 'mixs' or key == 'acl' or key == 'tags' or key == 'ver' or key == 'ns':
            continue
        if key == 'linkage':
            if len(doc[key].keys()) > 0:
                if count > 0:
                    createStr += ", "
                createStr += doc[key].keys()[0] + ": '" + str(doc[key][doc[key].keys()[0]][0]) + "'"
            else:
                continue
        else:
            if count > 0:
                createStr += ", "
            createStr += key + ": '" + str(doc[key]) + "'"
        count += 1
    for key in doc['meta'].keys():
        if key == 'meta' or key == 'mixs' or key == 'tags':
            continue
        createStr += ", "
        createStr += key + ": '" + ((str(doc['meta'][key]).replace("\'", "")).replace("\"", "")) + "'"
    for key in doc['acl'].keys():
        createStr += ", "
        createStr += "acl_" + key + ": '" + (str(doc['acl'][key][0]).replace("\'", "")) + "'"
    createStr += "})"
    cypher.run(createStr)
    if 'computed_from' in doc['linkage'].keys():
        for i in range(0, len(rawseqset16sIndices)):
             if doc['linkage']['computed_from'][0] == docList[int(rawseqset16sIndices[i])]['_id']:
                 relationshipStr = "MATCH (ra:RawSeqSet16s {_id: '" + docList[int(rawseqset16sIndices[i])]['_id'] + "'}), (tr:TrimmedSeqSet16s {_id:'" + doc['_id'] + "'}) CREATE (tr)-[:COMPUTED_FROM]->(ra)"
                 cypher.run(relationshipStr)
                 break
    k = k + 1
print("Created trimmedseqset16s " + str(k))
