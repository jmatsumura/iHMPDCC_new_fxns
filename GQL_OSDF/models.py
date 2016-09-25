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
    aclRead = graphene.List(graphene.String)
    aclWrite = graphene.List(graphene.String)

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
    partOf = graphene.List(graphene.String)
    
class Sample(graphene.ObjectType):
    class Meta:
        interfaces = (Defaults, )
    fmaBodySite = graphene.List(graphene.String)
    collectedDuring = graphene.List(graphene.String)
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

#print(graph.data(test))

def get_project(): # retrieve a single project (done once a project is decided upon)
    #from schema import Project # need to figure out circular dependency work around
    result = graph.data(test)
    print 'dummy'#(_id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

def get_study():
    print 'dummy'

def get_sample():
    print 'dummy'

#def get_all_projects(): # retrieve all projects

#def get_dnaprep16s():   

#def get_rawseqset16s():
    
#def get_subject():
    
#def get_trimmedseqset16s():

#def get_visit():
