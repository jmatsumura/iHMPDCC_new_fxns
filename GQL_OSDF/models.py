import graphene
from graphene import relay
from py2neo import Graph # Using py2neo v3 not v2

###################
# DEFINING MODELS #
###################

# This section will contain all the necessary models needed to populate the schema
#
# Each node will share properties within the Defaults class and have their own unique
# properties following it. Note that only the essential fields, which are those that would be 
# useful for querying the data, are extracted here. 

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

# Example ES query+result. Not really needed in this scenario, more important when doing custom queries.
#print(graph.run("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"iHMP\"}}}') YIELD node return node").data())

# Example normal Cypher query+result
#print(graph.data("MATCH (n:Project) RETURN n.name"))

# Function to build and run a Cypher query. Accepts the following parameters:
# attr = property to match against, val = desired value of the property of attr,
# links = an array with two elements [name of node to hit, name of edge].
# For example, for Study object you want to use the following parameters:
# buildQuery("node_type", "study", ["Project","PART_OF"])
def buildQuery(attr, val, links):
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

def get_project(): # retrieve all project node related data
    
    idl, nodeTypel, aclReadl, subtypel, namel, descriptionl = ([] for i in range(6)) # lists of each relevant query property
    res = buildQuery("node_type", "project", False)
    for x in range(0,len(res)):
        idl.append(res[x]['n']['id'])
        nodeTypel.append(res[x]['n']['node_type'])
        aclReadl.append(res[x]['n']['acl_read'])
        subtypel.append(res[x]['n']['subtype'])
        namel.append(res[x]['n']['name'])
        descriptionl.append(res[x]['n']['description'])
    return Project(ID=idl, nodeType=nodeTypel, aclRead=aclReadl, subtype=subtypel, name=namel, description=descriptionl)

def get_study():
    idl, nodeTypel, aclReadl, subtypel, centerl, contactl, namel, descriptionl, partOfl = ([] for i in range(9))
    res = buildQuery("null", "Study", ["Project","PART_OF"])
    for x in range(0,len(res)):
        idl.append(res[x]['b']['id'])
        nodeTypel.append(res[x]['b']['node_type'])
        aclReadl.append(res[x]['b']['acl_read'])
        subtypel.append(res[x]['b']['subtype'])
        centerl.append(res[x]['b']['center'])
        contactl.append(res[x]['b']['contact'])
        namel.append(res[x]['b']['name'])
        descriptionl.append(res[x]['b']['description'])
        if res[x]['link'] not in partOfl:
            partOfl.append(res[x]['link'])
    return Study(ID=idl, nodeType=nodeTypel, aclRead=aclReadl, subtype=subtypel, center=centerl, contact=contactl, name=namel, description=descriptionl, partOf=partOfl)

def get_sample():
    print 'dummy'

#def get_dnaprep16s():   

#def get_rawseqset16s():
    
#def get_subject():
    
#def get_trimmedseqset16s():

#def get_visit():
