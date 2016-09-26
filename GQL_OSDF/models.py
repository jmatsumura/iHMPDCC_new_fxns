import graphene
from graphene import relay
from py2neo import Graph # Using py2neo v3 not v2

###################
# DEFINING MODELS #
###################

# This section will contain all the necessary models needed to populate the schema

# Cannot inherit multiple levels, may need to just specify each class outright so that they
# can successfully inherit the [Node] interface
#class Defaults(graphene.Interface):   
    #id = graphene.ID()
    #nodeType = graphene.String()
    #aclRead = graphene.String() # this and the following likely unnecessary
    #aclWrite = graphene.String()

# Each node will have what makes it unique at the top and then generic/default values shared
# across all nodes following that. This will be restructured once what fields are absolutely
# essential are determined. 
#
# If edges are present between nodes, then these connections will be specified inbetween the
# different node types. 

class Defaults(graphene.Interface):
    ID = graphene.List(graphene.String)
    nodeType = graphene.List(graphene.String)

    # These next two are mostly needed for authentication purposes (if needed). Read is really only
    # relevant to the users while write is more of a backend thing. No need for write until proven otherwise
    aclRead = graphene.List(graphene.String)
    #aclWrite = graphene.List(graphene.String)

class Project(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    subtype = graphene.List(graphene.String)
    name = graphene.List(graphene.String)
    description = graphene.List(graphene.String)

class Study(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    subtype = graphene.List(graphene.String)
    center = graphene.List(graphene.String)
    contact = graphene.List(graphene.String)
    name = graphene.List(graphene.String)
    description = graphene.List(graphene.String)
    partOf = graphene.List(graphene.String) # part of what project

class Subject(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    race = graphene.List(graphene.String)
    gender = graphene.List(graphene.String)
    randSubjectId = graphene.List(graphene.String)
    participatesIn = graphene.List(graphene.String) # participates in what study

class Visit(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    date = graphene.List(graphene.String)
    interval = graphene.List(graphene.String)
    visitId = graphene.List(graphene.String)
    clinicId = graphene.List(graphene.String)
    visitNumber = graphene.List(graphene.String)
    by = graphene.List(graphene.String) # by what subject

class Sample(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    fmaBodySite = graphene.List(graphene.String)
    collectedDuring = graphene.List(graphene.String) # collected during what visit

class DNAPrep16s(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    #sequencingContact = graphene.List(graphene.String) # not a useful search condition?
    prepId = graphene.List(graphene.String)
    mimarks = graphene.List(graphene.String)
    libLayout = graphene.List(graphene.String)
    storageDuration = graphene.List(graphene.String)
    subtype = graphene.List(graphene.String)
    ncbiTaxonId = graphene.List(graphene.String)
    sequencingCenter = graphene.List(graphene.String)
    comment = graphene.List(graphene.String)
    libSelection = graphene.List(graphene.String)
    preparedFrom = graphene.List(graphene.String) # prepared from what sample

class RawSeqSet16s(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    #checksums = graphene.List(graphene.String) # not a useful search condition?
    #urls = graphene.List(graphene.String) # not a useful search condition?
    formatDoc = graphene.List(graphene.String)
    study = graphene.List(graphene.String)
    expLength = graphene.List(graphene.String)
    format = graphene.List(graphene.String)
    seqModel = graphene.List(graphene.String)
    sequenceType = graphene.List(graphene.String)
    size = graphene.List(graphene.String)
    subtype = graphene.List(graphene.String)
    sequencedFrom = graphene.List(graphene.String) # sequenced from what 16s prep

class RawSeqSet16s(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    #checksums = graphene.List(graphene.String) # not a useful search condition?
    #urls = graphene.List(graphene.String) # not a useful search condition?
    formatDoc = graphene.List(graphene.String)
    study = graphene.List(graphene.String)
    expLength = graphene.List(graphene.String)
    format = graphene.List(graphene.String)
    seqModel = graphene.List(graphene.String)
    sequenceType = graphene.List(graphene.String)
    size = graphene.List(graphene.String)
    subtype = graphene.List(graphene.String)
    comment = graphene.List(graphene.String)
    sequencedFrom = graphene.List(graphene.String) # sequenced from what 16s prep

class TrimmedSeqSet16s(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    #checksums = graphene.List(graphene.String) # not a useful search condition?
    #urls = graphene.List(graphene.String) # not a useful search condition?
    formatDoc = graphene.List(graphene.String)
    study = graphene.List(graphene.String)
    format = graphene.List(graphene.String)
    sequenceType = graphene.List(graphene.String)
    size = graphene.List(graphene.String)
    subtype = graphene.List(graphene.String)
    comment = graphene.List(graphene.String)
    computedFrom = graphene.List(graphene.String) # computed from what 16s raw seq set

##################
# CYPHER QUERIES #
##################

# This section will have all the logic for populating the actual data in the schema (data from Neo4j)

graph = Graph("http://localhost:7474/db/data/")

# Example ES query+result
#print(graph.run("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"iHMP\"}}}') YIELD node return node").data())

# Example normal Cypher query+result
#print(graph.data("MATCH (n:Project) RETURN n.name"))

attr = "node_type"
val = "project"
test = "CALL ga.es.queryNode('{\"query\":{\"match\":{\"%s\":\"%s\"}}}') YIELD node return node" % (attr, val)

# Function to build and run a Cypher (via ES) query. Accepts the following parameters:
# attr = property to match against, val = desired value of the property of attr,
# links = an array with two elements [name of node to hit, name of edge].
# For example, for Study object you want to use the following parameters:
# buildQuery("node_type", "study", ["project","part_of"])
def buildQuery(attr, val, links):
    if links:
        return 'hi'
    else:
        cquery = "CALL ga.es.queryNode('{\"query\":{\"match\":{\"%s\":\"%s\"}}}') YIELD node return node" % (attr, val)
        return graph.data(cquery)

def get_project(): # retrieve all project node related data
    
    idl, nodeTypel, aclReadl, subtypel, namel, descriptionl = ([] for i in range(6))
    res = buildQuery("node_type", "project", False)
    for x in range(0,len(res)):
        idl.append(res[x]['node']['id'])
        nodeTypel.append(res[x]['node']['node_type'])
        aclReadl.append(res[x]['node']['acl_read'])
        subtypel.append(res[x]['node']['subtype'])
        namel.append(res[x]['node']['name'])
        descriptionl.append(res[x]['node']['description'])
    return Project(ID=idl, nodeType=nodeTypel, aclRead=aclReadl, subtype=subtypel, name=namel, description=descriptionl)

def get_study():
    print 'dummy'

def get_sample():
    print 'dummy'

#def get_dnaprep16s():   

#def get_rawseqset16s():
    
#def get_subject():
    
#def get_trimmedseqset16s():

#def get_visit():
