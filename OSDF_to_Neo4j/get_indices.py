#!/usr/bin/python
#
# Script to build indices for parseJSONpy2neo.py.

import sys, re

uniqueNodes = {} # dictionary for finding and counting number of each node
regexForNode = r"\"node_type\":\"(\w+)\""

i = open(sys.argv[1], 'r') # couchdb dump json is the input

abundanceMatrix = open('./abundanceMatrixList.txt', 'w')
annotation = open('./annotationList.txt', 'w')
clusteredSeqSet = open('./clusteredSeqSetList.txt', 'w')
cytokine = open('./cytokineList.txt', 'w')
dnaPrep16S = open('./dnaPrep16SList.txt', 'w')
hostAssayPrep = open('./hostAssayPrepList.txt', 'w')
hostSeqPrep = open('./hostSeqPrepList.txt', 'w')
lipidome = open('./lipidomeList.txt', 'w')
metabolome = open('./metabolomeList.txt', 'w')
microbAssayPrep = open('./microbAssayPrepList.txt', 'w')
project = open('./projectList.txt', 'w')
proteome = open('./proteomeList.txt', 'w')
rawSeqSet16S = open('./rawSeqSet16SList.txt', 'w')
sample = open('./sampleList.txt', 'w')
sampleAttr = open('./sampleAttrList.txt', 'w')
sampleAttribute = open('./sampleAttributeList.txt', 'w')
study = open('./studyList.txt', 'w')
subject = open('./subjectList.txt', 'w')
trimmedSeqSet16S = open('./trimmedSeqSet16SList.txt', 'w')
viralSeqSet = open('./viralSeqSetList.txt', 'w')
visit = open('./visitList.txt', 'w')
visitAttr = open('./visitAttrList.txt', 'w')
wgsAssembledSeqSet = open('./wgsAssembledSeqSetList.txt', 'w')
wgsDNAPrep = open('./wgsDNAPrepList.txt', 'w')
wgsRawSeqSet = open('./wgsRawSeqSetList.txt', 'w')
wgsRawSeqSetPrivate = open('./wgsRawSeqSetPrivateList.txt', 'w')

k = -1 # offset since first line of dump can be skipped
for doc in i: # iterate over couchdb dump and extract indices
	
	if '"node_type"' in doc:
		node = re.search(regexForNode,doc).group(1)
		if not node in uniqueNodes:
			uniqueNodes[node] = 1
		else:
			uniqueNodes[node] += 1

	if '"node_type":"abundance_matrix"' in doc:
		abundanceMatrix.write(str(k)+'\n')
	elif '"node_type":"annotation"' in doc:
		annotation.write(str(k)+'\n')
	elif '"node_type":"clustered_seq_set"' in doc:
		clusteredSeqSet.write(str(k)+'\n')
	elif '"node_type":"cytokine"' in doc:
		cytokine.write(str(k)+'\n')
	elif '"node_type":"16s_dna_prep"' in doc:
		dnaPrep16S.write(str(k)+'\n')
	elif '"node_type":"host_assay_prep"' in doc:
		hostAssayPrep.write(str(k)+'\n')
	elif '"node_type":"host_seq_prep"' in doc:
		hostSeqPrep.write(str(k)+'\n')
	elif '"node_type":"lipidome"' in doc:
		lipidome.write(str(k)+'\n')
	elif '"node_type":"metabolome"' in doc:
		metabolome.write(str(k)+'\n')
	elif '"node_type":"microb_assay_prep"' in doc:
		microbAssayPrep.write(str(k)+'\n')
	elif '"node_type":"project"' in doc:
		project.write(str(k)+'\n')
	elif '"node_type":"proteome"' in doc:
		proteome.write(str(k)+'\n')
	elif '"node_type":"16s_raw_seq_set"' in doc:
		rawSeqSet16S.write(str(k)+'\n')
	elif '"node_type":"sample"' in doc:
		sample.write(str(k)+'\n')
	elif '"node_type":"sample_attr"' in doc:
		sampleAttr.write(str(k)+'\n')
	elif '"node_type":"sample_attribute"' in doc:
		sampleAttribute.write(str(k)+'\n')
	elif '"node_type":"study"' in doc:
		study.write(str(k)+'\n')
	elif '"node_type":"subject"' in doc:
		subject.write(str(k)+'\n')
	elif '"node_type":"16s_trimmed_seq_set"' in doc:
		trimmedSeqSet16S.write(str(k)+'\n')
	elif '"node_type":"viral_seq_set"' in doc:
		viralSeqSet.write(str(k)+'\n')
	elif '"node_type":"visit"' in doc:
		visit.write(str(k)+'\n')
	elif '"node_type":"visit_attr"' in doc:
		visitAttr.write(str(k)+'\n')
	elif '"node_type":"wgs_assembled_seq_set"' in doc:
		wgsAssembledSeqSet.write(str(k)+'\n')
	elif '"node_type":"wgs_dna_prep"' in doc:
		wgsDNAPrep.write(str(k)+'\n')
	elif '"node_type":"wgs_raw_seq_set"' in doc:
		wgsRawSeqSet.write(str(k)+'\n')
	elif '"node_type":"wgs_raw_seq_set_private"' in doc:
		wgsRawSeqSetPrivate.write(str(k)+'\n')

	k += 1

print "%s\t\t%s" % ("count","node")
for node in uniqueNodes:
	print "%s\t\t%s" % (uniqueNodes[node], node)