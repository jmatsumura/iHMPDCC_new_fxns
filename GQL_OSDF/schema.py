import graphene
from graphene import relay
from graphene.relay import Connection, Node


# Import the various Cypher queries to hit Neo4j and build the schema
from neo4j_query import get_project


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

class Project(graphene.ObjectType): # node

    class Meta:
        interfaces = [Node]

    subtype = graphene.String()
    name = graphene.String()
    description = graphene.String()
    _id = graphene.ID() # Defaults
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()


class PartOf(Connection): # edge

    class Meta:
        node = Project

    class Edge:
        rel_type = 'part_of'


class Study(graphene.ObjectType): # node

    class Meta:
        interfaces = [Node]
    projects = relay.ConnectionField(PartOf) # this expects a list of the node objects

    subtype = graphene.String()
    center = graphene.String()
    contact = graphene.String()
    name = graphene.String()
    description = graphene.String()
    _id = graphene.ID() # Defaults
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()


class ParticipatesIn(Connection): # edge

    class Meta:
        node = Study

    class Edge:
        rel_type = 'participates_in'


class Sample(graphene.ObjectType): # node

    class Meta:
        interfaces = [Node]
    studies = relay.ConnectionField(ParticipatesIn)

    fmaBodySite = graphene.String()
    _id = graphene.ID() # Defaults
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()


class Query(graphene.ObjectType): # grab everything! 

    project = graphene.Field(Project)
    study = graphene.Field(Study)
    sample = graphene.Field(Sample)
    node = relay.Node.Field() # get single Node if needed

    # Need this resolver to grab results that translate into Project classes
    def resolve_project(self, args, context, info):
        return Project(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

    def resolve_study(self, args, context, info):
        return Study(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='1', center='2', contact='yes', name='no', description='maybe', projects=[])
        
    def resolve_sample(self, args, context, info):
        return Sample(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', fmaBodySite='head', studies=[])       

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
