import re
import graphene
from py2neo import Graph # Using py2neo v3 not v2
from query import match, build_cypher

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

class IndivFiles(graphene.ObjectType): # individual files to populate all files list
    dataType = graphene.String(name="data_type")
    fileName = graphene.String(name="file_name")
    dataFormat = graphene.String(name="data_format")
    access = graphene.String() # only exists for consistency with GDC
    fileId = graphene.String(name="file_id")
    fileSize = graphene.Int(name="file_size")

class Analysis(graphene.ObjectType):
    updatedDatetime = graphene.String(name="updated_datetime")
    workflowType = graphene.String(name="workflow_type")
    analysisId = graphene.String(name="analysis_id")
    inputFiles = graphene.List(IndivFiles, name="input_files")

class AssociatedEntities(graphene.ObjectType):
    entityId = graphene.String(name="entity_id")
    caseId = graphene.String(name="case_id")
    entityType = graphene.String(name="entity_type")

class FileHits(graphene.ObjectType): # GDC defined file hits for data type, file name, data format, and more
    dataType = graphene.String(name="data_type")
    fileName = graphene.String(name="file_name")
    md5sum = graphene.String()
    dataFormat = graphene.String(name="data_format")
    submitterId = graphene.String(name="submitter_id")
    state = graphene.String()
    access = graphene.String()
    fileId = graphene.String(name="file_id")
    dataCategory = graphene.String(name="data_category")
    experimentalStrategy = graphene.String(name="experimental_strategy")
    fileSize = graphene.Float(name="file_size")
    cases = graphene.List(CaseHits)
    associatedEntities = graphene.List(AssociatedEntities, name="associated_entities")
    analysis = graphene.Field(Analysis)

class Bucket(graphene.ObjectType): # Each bucket is a distinct property in the node group
    key = graphene.String()
    docCount = graphene.Int(name="doc_count")

class BucketCounter(graphene.ObjectType): # List of Buckets
    buckets = graphene.List(Bucket)

class Aggregations(graphene.ObjectType): # Collecting lists of buckets (BucketCounter)
    Project_name = graphene.Field(BucketCounter)
    Sample_fmabodysite = graphene.Field(BucketCounter)
    Subject_gender = graphene.Field(BucketCounter)
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

# Function to extract known GDC syntax and convert to OSDF. This is commonly needed for performing
# cypher queries while still being able to develop the front-end with the cases syntax.
def convert_gdc_to_osdf(inp_str):
    # Errors in Graphene mapping prevent the syntax I want, so ProjectName is converted to 
    # Cypher ready Project.name here (as are the other possible query parameters).
    inp_str = inp_str.replace("cases.ProjectName","Project.name")
    inp_str = inp_str.replace("cases.SampleFmabodysite","Sample.body_site")
    inp_str = inp_str.replace("cases.SubjectGender","Subject.gender")
    inp_str = inp_str.replace("project.primary_site","Sample.body_site")
    inp_str = inp_str.replace("subject.gender","Subject.gender")
    inp_str = inp_str.replace("files.file_id","sf._id")
    # Next two lines guarantee URL encoding (seeing errors with urllib and hacking for demo)
    inp_str = inp_str.replace('"','|')
    inp_str = inp_str.replace(" ","%20")
    return inp_str

# Function to get file size from Neo4j. 
# This current iteration should catch all the file data types EXCEPT for the *omes and the multi-step/repeat
# edges like the two "computed_from" edges between abundance matrix and 16s_raw_seq_set. Should be
# rather easy to accommodate these oddities once they're loaded and I can test.
def get_total_file_size(cy):
    cquery = ""
    if cy == "":
        cquery = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(p)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) RETURN (SUM(toInt(sf.size))+SUM(toInt(cf.size))) as tot"
    else:
        cquery = build_cypher(match,cy,"null","null","null","size")
    res = graph.data(cquery)
    return res[0]['tot']

# Function for pagination calculations. Find the page, number of pages, and number of entries on a single page.
def pagination_calcs(total,start,size,c_or_f):
    pg,pgs,cnt,tot = (0 for i in range(4))
    if c_or_f == "c":
        tot = int(total/2) # one case per two types of files currently
        sort = "case_id.raw:asc"      
    else:
        tot = int(total)
        sort = "file_name.raw:asc"
    if size != 0: pgs = int(tot / size) + (tot % size > 0)
    if size != 0: pg = int(start / size) + (start % size > 0)
    if (start+size) < tot: # less than full page, count must be page size
        cnt = size
    else: # if less than a full page (only possible on last page), find the difference
        cnt = tot-start
    pagcalcs = []
    pagcalcs.append(pgs)
    pagcalcs.append(pg)
    pagcalcs.append(cnt)
    pagcalcs.append(tot)
    pagcalcs.append(sort)
    return pagcalcs

# Function to determine how pagination is to work for the cases/files tabs. This will 
# take a Cypher query and a given table size and determine how many pages are needed
# to display all this data. 
# cy = Cypher filters/ops
# size = size of each page
# f = from/start position
def get_pagination(cy,size,f,c_or_f):
    if cy == "":
        cquery = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(p)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) RETURN (count(sf)+count(cf)) AS tot"
        res = graph.data(cquery)
        calcs = pagination_calcs(res[0]['tot'],f,size,c_or_f)
        return Pagination(count=calcs[2], sort=calcs[4], fromNum=f, page=calcs[1], total=calcs[3], pages=calcs[0], size=size)
    else:
        cquery = cquery = build_cypher(match,cy,"null","null","null","pagination")
        res = graph.data(cquery)
        calcs = pagination_calcs(res[0]['tot'],f,size,c_or_f)
        return Pagination(count=calcs[2], sort=calcs[4], fromNum=f, page=calcs[1], total=calcs[3], pages=calcs[0], size=size)

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
def count_props(node, prop, cy):
    cquery = ""
    if cy == "":
        cquery = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(pf)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) RETURN %s.%s as prop, count(%s.%s) as counts" % (node, prop, node, prop)
    else:
        cquery = build_cypher(match,cy,"null","null","null",prop)
    return graph.data(cquery)

# Formats the values from the count_props function above into GQL
def get_buckets(inp,sum, cy):

    splits = inp.split('.') # parse for node/prop values to be counted by
    node = splits[0]
    prop = splits[1]
    bucketl = []

    if sum == "no": # not a full summary, just key and doc count need to be returned
        res = count_props(node, prop, cy)
        for x in range(0,len(res)):
            if res[x]['prop'] != "":
                cur = Bucket(key=res[x]['prop'], docCount=res[x]['counts'])
                bucketl.append(cur)

        return BucketCounter(buckets=bucketl)

    else: # return full summary including case_count, doc_count, file_size, and key
        res = count_props(node, prop, cy)
        for x in range(0,len(res)):
            if res[x]['prop'] != "":
                cur = SBucket(key=res[x]['prop'], docCount=res[x]['counts'], fileSize=res[x]['counts'], caseCount=res[x]['counts'])
                bucketl.append(cur)

        return SBucketCounter(buckets=bucketl)

# Function to return case values to populate the table, note that this will just return first 25 values arbitrarily for the moment
# size = number of hits to return
# order = what to ORDER BY in Cypher clause
# f = position to star the return 'f'rom based on the ordering (python prevents using that word)
# cy = filters/op sent from GDC portal
def get_case_hits(size,order,f,cy):
    hits = []
    cquery = ""
    if cy == "":
        order = order.split(":")
        cquery = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(pf)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) RETURN Project.name,Project.subtype,Sample.body_site,Sample._id ORDER BY %s %s SKIP %s LIMIT %s" % (order[0],order[1].upper(),f-1,size)
    else:
        cquery = build_cypher(match,cy,order,f,size,"cases")
    res = graph.data(cquery)
    for x in range(0,len(res)):
        cur = CaseHits(project=Project(projectId=res[x]['Project.subtype'],primarySite=res[x]['Sample.body_site'],name=res[x]['Project.name'],diseaseType="demo"),caseId=res[x]['Sample._id'])
        hits.append(cur)
    return hits

# Function to return file values to populate the table, note that this will just return first 30 values arbitrarily for the moment
# Note that the way this is performed, guaranteed a trimmed set from a raw set so pulling 15 and pulling one file from each node (=30)
def get_file_hits(size,order,f,cy):
    hits = []
    f = int(f / 2) + (f % 2 > 0)
    cquery = ""
    if cy == "":
        order = order.split(":")
        cquery = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(prep)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) RETURN Project,sf,cf,Sample._id ORDER BY %s %s SKIP %s LIMIT %s" % (order[0],order[1].upper(),f-1,size/2)
    else:
        cquery = build_cypher(match,cy,order,f,size/2,"files")
    res = graph.data(cquery)
    for x in range(0,len(res)):
        case_hits = [] # reinit each iteration
        cur_case = CaseHits(project=Project(projectId=res[x]['Project']['subtype'],name=res[x]['Project']['name']),caseId=res[x]['Sample._id'])
        case_hits.append(cur_case)
        fn_s = extract_url(res[x]['sf']['urls']) # File name is our URL
        fn_c = extract_url(res[x]['cf']['urls'])
        cur_file1 = FileHits(dataType=res[x]['sf']['subtype'],fileName=fn_s,dataFormat=res[x]['sf']['format'],submitterId="null",access="open",state="submitted",fileId=res[x]['sf']['_id'],dataCategory=res[x]['sf']['node_type'],experimentalStrategy=res[x]['sf']['subtype'],fileSize=res[x]['sf']['size'],cases=case_hits)
        cur_file2 = FileHits(dataType=res[x]['cf']['subtype'],fileName=fn_c,dataFormat=res[x]['cf']['format'],submitterId="null",access="open",state="submitted",fileId=res[x]['cf']['_id'],dataCategory=res[x]['cf']['node_type'],experimentalStrategy=res[x]['cf']['subtype'],fileSize=res[x]['cf']['size'],cases=case_hits) 
        hits.append(cur_file1)
        hits.append(cur_file2)       
    return hits

# The following section is needed to pull data from the different file types/nodes.
# How it aims to work is given a file ID, it will check Neo4j for the type then use
# this knowledge to build a smarter Cypher query that will return the relevant data.
# The logic here will be much easier to follow then writing one huge catch-all Neo4j
# query and then parsing it afterwards.
# next line is placeholder graphene object that will be reused
#return FileHits(dataType=,fileName=fn,md5sum=,dataFormat=,submitterId="",state="submitted",access="open",fileId=,dataCategory=,experimentalStrategy=,fileSize=,cases=,associatedEntities=,analysis=)
def get_16s_raw_seq_set(id):
    cl, al, fl = ([] for i in range(3))
    cquery = "MATCH (p:Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(b:Sample)<-[:PREPARED_FROM]-(prep)<-[:SEQUENCED_FROM]-(s)<-[:COMPUTED_FROM]-(c) WHERE s._id=\"%s\" RETURN p,prep,s,c,b" % (id)
    res = graph.data(cquery)
    fn = extract_url(res[0]['s']['urls'])
    wf = "%s -> %s" % (res[0]['prep']['subtype'],res[0]['s']['subtype']) # this WF could be quite revealing, decide a more complete definition later
    cl.append(CaseHits(project=Project(projectId=res[0]['p']['subtype']),caseId=res[0]['b']['_id']))
    al.append(AssociatedEntities(entityId=res[0]['prep']['_id'],caseId=res[0]['b']['_id'],entityType="prep"))
    al.append(AssociatedEntities(entityId=res[0]['c']['_id'],caseId=res[0]['b']['_id'],entityType="trimmed set")) 
    fl.append(IndivFiles(fileId="null"))
    a = Analysis(updatedDatetime="null",workflowType=wf,analysisId="null",inputFiles=fl)
    return FileHits(dataType=res[0]['s']['subtype'],fileName=fn,md5sum=res[0]['s']['checksums'],dataFormat=res[0]['s']['format'],submitterId="null",state="submitted",access="open",fileId=res[0]['s']['_id'],dataCategory="16S",experimentalStrategy=res[0]['s']['study'],fileSize=res[0]['s']['size'],cases=cl,associatedEntities=al,analysis=a)

def get_16s_trimmed_seq_set(id):
    cl, al, fl = ([] for i in range(3)) # case, associated entity, and input file lists
    cquery = "MATCH (p:Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(b:Sample)<-[:PREPARED_FROM]-(prep)<-[:SEQUENCED_FROM]-(s)<-[:COMPUTED_FROM]-(c) WHERE c._id=\"%s\" RETURN p,prep,s,c,b" % (id)
    res = graph.data(cquery)
    fn = extract_url(res[0]['c']['urls'])
    wf = "%s -> %s -> %s" % (res[0]['prep']['subtype'],res[0]['s']['subtype'],res[0]['c']['subtype']) # this WF could be quite revealing, decide a more complete definition later
    cl.append(CaseHits(project=Project(projectId=res[0]['p']['subtype']),caseId=res[0]['b']['_id']))
    al.append(AssociatedEntities(entityId=res[0]['prep']['_id'],caseId=res[0]['b']['_id'],entityType="prep"))
    al.append(AssociatedEntities(entityId=res[0]['s']['_id'],caseId=res[0]['b']['_id'],entityType="raw set"))
    fl.append(IndivFiles(fileId=res[0]['s']['_id']))
    a = Analysis(updatedDatetime="null",workflowType=wf,analysisId="null",inputFiles=fl) # can add analysis ID once node is present or remove if deemed unnecessary
    return FileHits(dataType=res[0]['c']['subtype'],fileName=fn,md5sum=res[0]['c']['checksums'],dataFormat=res[0]['c']['format'],submitterId="null",state="submitted",access="open",fileId=res[0]['c']['_id'],dataCategory="16S",experimentalStrategy=res[0]['c']['study'],fileSize=res[0]['c']['size'],cases=cl,associatedEntities=al,analysis=a)

options = {'16s_raw_seq_set': get_16s_raw_seq_set,
    '16s_trimmed_seq_set': get_16s_trimmed_seq_set,
}

def get_file_data(file_id):
    cquery = "MATCH (n) WHERE n._id=\"%s\" RETURN n.node_type AS type" % (file_id)
    res = graph.data(cquery)
    node = res[0]['type']
    final_res = options[node](file_id)
    return final_res

def get_url_for_download(id):
    cquery = "MATCH (n) WHERE n._id=\"%s\" RETURN n.urls AS urls" % (id)
    res = graph.data(cquery)
    return extract_url(res[0]['urls'])
    