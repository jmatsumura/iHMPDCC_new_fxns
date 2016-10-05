import re
import graphene
from graphene import relay
from py2neo import Graph # Using py2neo v3 not v2

###################
# DEFINING MODELS #
###################

# This section will contain all the necessary models needed to populate the schema

class Project(graphene.ObjectType): # Graphene object for node
    projectId = graphene.String(name="project_id")
    primarySite = graphene.String(name="primary_site")
    name = graphene.String()
    diseaseType = graphene.String(name="disease_type")

class Pagination(graphene.ObjectType): # GDC expects pagination data for populating table
    count = graphene.Int()
    sort = graphene.String()
    fromNum = graphene.Int(name="from")
    page = graphene.Int()
    total = graphene.Int()
    pages = graphene.Int()
    size = graphene.Int()

class CaseHits(graphene.ObjectType): # GDC defines hits as matching Project node + Case ID (in our case sample ID)
    project = graphene.Field(Project)
    caseId = graphene.String(name="case_id")

class FileHits(graphene.ObjectType): # GDC defined file hits for data type, file name, data format, and more
    dataType = graphene.String(name="data_type")
    fileName = graphene.String(name="file_name")
    dataFormat = graphene.String(name="data_format")
    submitterId = graphene.String(name="submitter_id")
    state = graphene.String()
    access = graphene.String()
    fileId = graphene.String(name="file_id")
    dataCategory = graphene.String(name="data_category")
    experimentalStrategy = graphene.String(name="experimental_strategy")
    fileSize = graphene.Int(name="file_size")
    cases = graphene.List(CaseHits)

class Bucket(graphene.ObjectType): # Each bucket is a distinct property in the node group
    key = graphene.String()
    docCount = graphene.Int(name="doc_count")

class BucketCounter(graphene.ObjectType): # List of Buckets
    buckets = graphene.List(Bucket)

class Aggregations(graphene.ObjectType): # Collecting lists of buckets (BucketCounter)
    Project_name = graphene.Field(BucketCounter)
    Sample_fmabodysite = graphene.Field(BucketCounter)
    dataType = graphene.Field(BucketCounter, name="data_type")
    dataFormat = graphene.Field(BucketCounter, name="data_format")

class SBucket(graphene.ObjectType): # Same idea as early bucket but used for summation (pie charts)
    key = graphene.String()
    docCount = graphene.Int(name="doc_count")
    caseCount = graphene.Int(name="case_count")
    fileSize = graphene.Int(name="file_size")

class SBucketCounter(graphene.ObjectType): # List of SBuckets
    buckets = graphene.List(SBucket)

class FileSize(graphene.ObjectType): # total aggregate file size of current set of chosen data
    value = graphene.Float()

class IndivFiles(graphene.ObjectType): # individual files to populate all files list
    dataType = graphene.String(name="data_type")
    fileName = graphene.String(name="file_name")
    dataFormat = graphene.String(name="data_format")
    access = graphene.String() # only exists for consistency with GDC
    fileId = graphene.String(name="file_id")
    fileSize = graphene.Int(name="file_size")

####################################
# FUNCTIONS FOR GETTING NEO4J DATA #
####################################

# This section will have all the logic for populating the actual data in the schema (data from Neo4j)

graph = Graph("http://localhost:7474/db/data/")

# Function to extract a file name and an HTTP URL given values from a urls property from an OSDF node
def extract_url(urls_node):
    regex_for_http_urls = '(http.*data/\S+)[,\]]'
    fn = ""
    if re.match('.*http.*', urls_node):
        name_and_url = re.search(regex_for_http_urls, urls_node)
        fn = name_and_url.group(1) # File name is our URL
    else:
        fn = "none"
    return fn

# Function to get file size from Neo4j. 
# This current iteration should catch all the file data types EXCEPT for the *omes and the multi-step/repeat
# edges like the two "computed_from" edges between abundance matrix and 16s_raw_seq_set. Should be
# rather easy to accommodate these oddities once they're loaded and I can test.
def get_total_file_size():
    cquery = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(p)<-[:SEQUENCED_FROM]-(s)<-[:COMPUTED_FROM]-(c) RETURN (SUM(toInt(s.size))+SUM(toInt(c.size))) as tot"
    res = graph.data(cquery)
    return res[0]['tot']

# Function to build and run a basic Cypher query. Accepts the following parameters:
# attr = property to match against, val = desired value of the property of attr,
# links = an array with two elements [name of node to hit, name of edge].
# For example, for Study object you want to use the following parameters:
# buildQuery("node_type", "Study", ["Project","PART_OF"])
# Note that this is a single-step query meaning 2 nodes and 1 edge
def build_basic_query(attr, val, links):
    if links:
        node = links[0] # parse links array as described earlier, don't need attr
        edge = links[1]
        # Note that collecting distinct nodes connected by the edge doesn't help much
        # since each unique originating node comes paired with a link. Thus, check for
        # unique-ness when appending to lists in the get_* functions below
        cquery = "MATCH (a:%s)<-[:%s]-(b:%s) RETURN a.name AS link, b" % (node, edge, val)
        return graph.data(cquery)
    else:
        #cquery = "CALL ga.es.queryNode('{\"query\":{\"match\":{\"%s\":\"%s\"}}}') YIELD node RETURN node" % (attr, val)
        cquery = "MATCH (n {%s: '%s'}) RETURN n" % (attr, val)
        return graph.data(cquery)

# Retrieve ALL files associated with a given sample_id. Note the generic node names p, s, and c.
# Overall this means start at the (b)eginning (b:Sample), get all associated (p)repared from nodes,
# then all (s)equenced from nodes, and finally all (c)omputed from nodes. The attributes to find and
# return are the URL, name, size, format, and type. 
def get_files(sample_id):
    fl = []
    dt, fn, df, ac, fi = ("" for i in range(5))
    fs = 0
    regex_for_http_urls = '\,\su(http.*data/(.*))\,'
    pattern = re.compile(regex_for_http_urls)
    
    cquery = "MATCH (b:Sample)<-[:PREPARED_FROM]-(p)<-[:SEQUENCED_FROM]-(s)<-[:COMPUTED_FROM]-(c) WHERE b._id=\"%s\" RETURN s, c" % (sample_id)
    res = graph.data(cquery)

    for x in range(0,len(res)): # iterate over each unique path
        for key in res[x].keys(): # iterate over each unique node in the path
            dt = res[x][key]['subtype']
            df = res[x][key]['format']
            ac = "open" # again, default to accommodate current GDC format
            fs = res[x][key]['size']
            fi = res[x][key]['_id']
            fn = extract_url(res[x][key]['urls'])
            fl.append(IndivFiles(dataType=dt,fileName=fn,dataFormat=df,access=ac,fileId=fi,fileSize=fs))

    return fl

# Query to traverse top half of OSDF model (Project<-....-Sample). 
def get_proj_data(sample_id):
    cquery = "MATCH (p:Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(SUBJECT)<-[:BY]-(VISIT)<-[:COLLECTED_DURING]-(Sample) WHERE Sample._id=\"%s\" RETURN p" % (sample_id)
    res = graph.data(cquery)
    return Project(name=res[0]['p']['name'],projectId=res[0]['p']['subtype'])

# Cypher query to count the amount of each distinct property
def count_props(node, prop):
    cquery = "MATCH (n:%s) RETURN n.%s as prop, count(n.%s) as counts" % (node, prop, prop)
    return graph.data(cquery)

# Formats the values from the count_props function above into GQL
def get_buckets(inp,sum):

    splits = inp.split('.') # parse for node/prop values to be counted by
    node = splits[0]
    prop = splits[1]
    bucketl = []

    if sum == "no": # not a full summary, just key and doc count need to be returned
        res = count_props(node, prop)
        for x in range(0,len(res)):
            if res[x]['prop'] != "":
                cur = Bucket(key=res[x]['prop'], docCount=res[x]['counts'])
                bucketl.append(cur)

        return BucketCounter(buckets=bucketl)

    else: # return full summary including case_count, doc_count, file_size, and key
        res = count_props(node, prop)
        for x in range(0,len(res)):
            if res[x]['prop'] != "":
                cur = SBucket(key=res[x]['prop'], docCount=res[x]['counts'], fileSize=res[x]['counts'], caseCount=res[x]['counts'])
                bucketl.append(cur)

        return SBucketCounter(buckets=bucketl)

# Function to return case values to populate the table, note that this will just return first 25 values arbitrarily for the moment
def get_case_hits():
    hits = []
    cquery = "MATCH (p:Project)<-[:PART_OF]-(st:Study)<-[:PARTICIPATES_IN]-(su:Subject)<-[:BY]-(v:Visit)<-[:COLLECTED_DURING]-(sa:Sample) WHERE NOT sa.fma_body_site=\"\" RETURN p.name,p.subtype,sa.fma_body_site,st.name,sa._id LIMIT 25"
    res = graph.data(cquery)
    for x in range(0,len(res)):
        cur = CaseHits(project=Project(projectId=res[x]['p.subtype'],primarySite=res[x]['sa.fma_body_site'],name=res[x]['p.name'],diseaseType="demo"),caseId=res[x]['sa._id'])
        hits.append(cur)
    return hits

# Function to return file values to populate the table, note that this will just return first 30 values arbitrarily for the moment
# Note that the way this is performed, guaranteed a trimmed set from a raw set so pulling 15 and pulling one file from each node (=30)
def get_file_hits():
    hits = []
    cquery = "MATCH (p:Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(b:Sample)<-[:PREPARED_FROM]-(prep)<-[:SEQUENCED_FROM]-(s)<-[:COMPUTED_FROM]-(c) RETURN p,s,c,b._id LIMIT 15"
    res = graph.data(cquery)
    for x in range(0,len(res)):
        case_hits = [] # reinit each iteration
        cur_case = CaseHits(project=Project(projectId=res[x]['p']['subtype'],name=res[x]['p']['name']),caseId=res[x]['b._id'])
        case_hits.append(cur_case)
        fn_s = extract_url(res[x]['s']['urls']) # File name is our URL
        fn_c= extract_url(res[x]['c']['urls'])
        cur_file1 = FileHits(dataType=res[x]['s']['subtype'],fileName=fn_s,dataFormat=res[x]['s']['format'],submitterId="null",access="open",state="submitted",fileId=res[x]['s']['_id'],dataCategory=res[x]['s']['node_type'],experimentalStrategy=res[x]['s']['subtype'],fileSize=res[x]['s']['size'],cases=case_hits)
        cur_file2 = FileHits(dataType=res[x]['c']['subtype'],fileName=fn_c,dataFormat=res[x]['c']['format'],submitterId="null",access="open",state="submitted",fileId=res[x]['c']['_id'],dataCategory=res[x]['c']['node_type'],experimentalStrategy=res[x]['c']['subtype'],fileSize=res[x]['c']['size'],cases=case_hits) 
        hits.append(cur_file1)
        hits.append(cur_file2)       
    return hits


###########
# DEV/TMP #
###########

def get_hits():
    hits = []
    s1 = Hits(project=Project(projectId="123", primarySite="head", name="1", diseaseType="RA"),caseId="3674d95cd0d27e1de94ddf4d2eccecc3")
    s2 = Hits(project=Project(projectId="456", primarySite="shoulders", name="12", diseaseType="RB"),caseId="e2559e04fcd73935a7d7b9179041782f")
    s3 = Hits(project=Project(projectId="789", primarySite="knees", name="13", diseaseType="RC"),caseId="e2559e04fcd73935a7d7b9179073a82e")
    hits.append(s1)
    hits.append(s2)
    hits.append(s3)
    return hits



##############
# DEPRECATED #
##############

class Defaults(graphene.Interface):
    ID = graphene.List(graphene.String)
    #n odeType is also redundant due to how the GQL statements are named (e.g. project root of gql query
    # will always have nodeType == 'project')
    #nodeType = graphene.List(graphene.String)

    # These next two are mostly needed for authentication purposes (if needed). Read is really only
    # relevant to the users while write is more of a backend thing. No need for write until proven otherwise.
    # If we want security, should create two seperate GQL endpoints (one which only presents the aclRead=='read')
    #aclRead = graphene.List(graphene.String) 
    #aclWrite = graphene.List(graphene.String)

class Project_old(graphene.ObjectType): # used to be named Project and accommodated all the Graphene objects below
    class Meta:
        interfaces = (Defaults, )
    subtype = graphene.List(graphene.String)
    name = graphene.List(graphene.String)
    description = graphene.List(graphene.String)

class Study(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    project = graphene.List(Project)
    subtype = graphene.List(graphene.String)
    center = graphene.List(graphene.String)
    contact = graphene.List(graphene.String)
    name = graphene.List(graphene.String)
    description = graphene.List(graphene.String)
    partOf = graphene.List(graphene.String) # part of what project

# Below are functions to extract all the data related to a particular node that might
# be worth searching. Since this is meant to populate auto-complete text, the fields
# where there are likely redundant (will only add to the clutter of options) values
# are checked for in any of the 'if' statements.

def get_project(): # retrieve all project node related data
    idl, subtypel, namel, descriptionl = ([] for i in range(4)) # lists of each relevant query property
    res = build_basic_query("node_type", "project", False)
    for x in range(0,len(res)):
        idl.append(res[x]['n']['id']) # need to switch ALL to 'id' or '_id'
        subtypel.append(res[x]['n']['subtype'])
        namel.append(res[x]['n']['name'])
        descriptionl.append(res[x]['n']['description'])
    return Project(ID=idl, subtype=subtypel, name=namel, description=descriptionl)

def get_study():
    idl, subtypel, centerl, contactl, namel, descriptionl, partOfl = ([] for i in range(7))
    res = build_basic_query("null", "Study", ["Project","PART_OF"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        if res[x]['b']['subtype'] is not None:
            if res[x]['b']['subtype'] not in subtypel:
                subtypel.append(res[x]['b']['subtype'])
        if res[x]['b']['center'] not in centerl: centerl.append(res[x]['b']['center'])
        if res[x]['b']['contact'] not in contactl: contactl.append(res[x]['b']['contact'])
        namel.append(res[x]['b']['name'])
        descriptionl.append(res[x]['b']['description'])
        if res[x]['link'] not in partOfl: partOfl.append(res[x]['link'])
    return Study(ID=idl, subtype=subtypel, center=centerl, contact=contactl, name=namel, description=descriptionl, partOf=partOfl)

def get_subject():
    idl, racel, genderl, randSubjectIdl, participatesInl = ([] for i in range(5))
    res = build_basic_query("null", "Subject", ["Study","PARTICIPATES_IN"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        if res[x]['b']['race'] is not None:
            if res[x]['b']['race'] not in racel:
                racel.append(res[x]['b']['race'])
        if res[x]['b']['gender'] not in genderl: genderl.append(res[x]['b']['gender'])
        randSubjectIdl.append(res[x]['b']['rand_subject_id'])
        if res[x]['link'] not in participatesInl: participatesInl.append(res[x]['link'])
    return Subject(ID=idl, race=racel, gender=genderl, randSubjectId=randSubjectIdl, participatesIn=participatesInl)

def get_visit():
    idl, datel, intervall, visitIdl, clinicIdl, visitNumberl, byl = ([] for i in range(7))
    res = build_basic_query("null", "Visit", ["Subject","BY"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        if res[x]['b']['date'] is not None: 
            if res[x]['b']['date'] not in datel:
                datel.append(res[x]['b']['date'])
        if res[x]['b']['interval'] not in intervall: intervall.append(res[x]['b']['interval'])
        visitIdl.append(res[x]['b']['visit_id'])
        clinicIdl.append(res[x]['b']['clinic_id'])
        if res[x]['b']['visit_number'] not in visitNumberl: visitNumberl.append(res[x]['b']['visit_number'])
        if res[x]['link'] not in byl: byl.append(res[x]['link'])
    return Visit(ID=idl, date=datel, interval=intervall, visitId=visitIdl, clinicId=clinicIdl, visitNumber=visitNumberl, by=byl)

def get_sample():
    idl, fmaBodySitel, collectedDuringl = ([] for i in range(3))
    res = build_basic_query("null", "Sample", ["Visit","COLLECTED_DURING"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        if res[x]['b']['fma_body_site'] not in fmaBodySitel:
            if res[x]['b']['fma_body_site'] != "":
                fmaBodySitel.append(res[x]['b']['fma_body_site'])
        if res[x]['link'] not in collectedDuringl: collectedDuringl.append(res[x]['link'])
    return Sample(ID=idl, fmaBodySite=fmaBodySitel, collectedDuring=collectedDuringl)

def get_dnaprep16s():
    idl, prepIdl, libLayoutl, storageDurationl, subtypel, ncbiTaxonIdl, sequencingCenterl, commentl, libSelectionl, preparedFroml = ([] for i in range(10))
    res = build_basic_query("null", "DNAPrep16s", ["Sample","PREPARED_FROM"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        prepIdl.append(res[x]['b']['prep_id'])
        if res[x]['b']['lib_layout'] not in libLayoutl: libLayoutl.append(res[x]['b']['lib_layout'])
        if res[x]['b']['storage_duration'] not in storageDurationl: storageDurationl.append(res[x]['b']['storage_duration'])
        if res[x]['b']['subtype'] not in subtypel: subtypel.append(res[x]['b']['subtype'])
        if res[x]['b']['ncbi_taxon_id'] not in ncbiTaxonIdl: ncbiTaxonIdl.append(res[x]['b']['ncbi_taxon_id'])
        if res[x]['b']['sequencing_center'] not in sequencingCenterl: sequencingCenterl.append(res[x]['b']['sequencing_center'])
        if res[x]['b']['lib_layout'] not in libLayoutl: libLayoutl.append(res[x]['b']['lib_layout'])
        if res[x]['b']['comment'] is not None: 
            if res[x]['b']['comment'] not in commentl:
                commentl.append(res[x]['b']['comment'])
        if res[x]['b']['lib_selection'] not in libSelectionl: libSelectionl.append(res[x]['b']['lib_selection'])
        if res[x]['link'] not in preparedFroml: preparedFroml.append(res[x]['link'])
    return DNAPrep16s(ID=idl, prepId=prepIdl, libLayout=libLayoutl, storageDuration=storageDurationl, subtype=subtypel, ncbiTaxonId=ncbiTaxonIdl, sequencingCenter=sequencingCenterl, comment=commentl, libSelection=libSelectionl, preparedFrom=preparedFroml)

def get_rawseqset16s():
    idl, formatDocl, studyl, expLengthl, formatl, seqModell, seqTypel, sizel, subtypel, commentl, sequencedFroml = ([] for i in range(11))
    res = build_basic_query("null", "RawSeqSet16s", ["DNAPrep16s","SEQUENCED_FROM"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        if res[x]['b']['format_doc'] is not None: 
            if res[x]['b']['format_doc'] not in formatDocl:
                formatDocl.append(res[x]['b']['format_doc'])
        if res[x]['b']['study'] not in studyl: studyl.append(res[x]['b']['study'])
        if res[x]['b']['exp_length'] not in expLengthl: expLengthl.append(res[x]['b']['exp_length'])
        if res[x]['b']['format'] not in formatl: formatl.append(res[x]['b']['format'])
        if res[x]['b']['seq_model'] is not None: 
            if res[x]['b']['seq_model'] not in seqModell:
                seqModell.append(res[x]['b']['seq_model'])
        if res[x]['b']['sequence_type'] is not None:
            if res[x]['b']['sequence_type'] not in seqTypel: 
                seqTypel.append(res[x]['b']['sequence_type'])
        if res[x]['b']['size'] not in sizel: sizel.append(res[x]['b']['size'])
        if res[x]['b']['subtype'] not in subtypel: subtypel.append(res[x]['b']['subtype'])
        if res[x]['b']['comment'] is not None: 
            if res[x]['b']['comment'] not in commentl:
                commentl.append(res[x]['b']['comment'])
        if res[x]['link'] not in sequencedFroml: sequencedFroml.append(res[x]['link'])
    return RawSeqSet16s(ID=idl, formatDoc=formatDocl, study=studyl, expLength=expLengthl, format=formatl, seqModel=seqModell, seqType=seqTypel, size=sizel, subtype=subtypel, comment=commentl, sequencedFrom=sequencedFroml)

def get_trimmedseqset16s():
    idl, formatDocl, studyl, formatl, seqTypel, sizel, subtypel, commentl, computedFroml = ([] for i in range(9))
    res = build_basic_query("null", "TrimmedSeqSet16s", ["RawSeqSet16s","COMPUTED_FROM"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['_id'])
        if res[x]['b']['format_doc'] is not None: 
            if res[x]['b']['format_doc'] not in formatDocl:
                formatDocl.append(res[x]['b']['format_doc'])
        if res[x]['b']['study'] not in studyl: studyl.append(res[x]['b']['study'])
        if res[x]['b']['format'] not in formatl: formatl.append(res[x]['b']['format'])
        if res[x]['b']['sequence_type'] is not None:
            if res[x]['b']['sequence_type'] not in seqTypel: 
                seqTypel.append(res[x]['b']['sequence_type'])
        if res[x]['b']['size'] not in sizel: sizel.append(res[x]['b']['size'])
        if res[x]['b']['subtype'] not in subtypel: subtypel.append(res[x]['b']['subtype'])
        if res[x]['b']['comment'] is not None: 
            if res[x]['b']['comment'] not in commentl:
                commentl.append(res[x]['b']['comment'])
        if res[x]['link'] not in computedFroml: computedFroml.append(res[x]['link'])
    return TrimmedSeqSet16s(ID=idl, formatDoc=formatDocl, study=studyl, format=formatl, seqType=seqTypel, size=sizel, subtype=subtypel, comment=commentl, computedFrom=computedFroml)
