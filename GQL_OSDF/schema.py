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

class Project(graphene.ObjectType):

    class Meta:
        interfaces = [Node]

    subtype = graphene.String()
    name = graphene.String()
    description = graphene.String()
    # Defaults
    _id = graphene.ID()
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()


class Sample(graphene.ObjectType):

    class Meta:
        interfaces = [Node]

    fmaBodySite = graphene.String()
    # Defaults
    _id = graphene.ID()
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()

# Cover edges here, note that we probably need more than just what is provided by iHMP if we want
# to group things like Projects under one branch.
class PartOf(Connection):

    class Meta:
        node = Project

    class Edge:
        rel_type = 'part_of'


class Query(graphene.ObjectType):

    project = graphene.Field(Project)
    sample = graphene.Field(Sample)
    node = relay.Node.Field() # get single Node if needed

    # Need this resolver to grab results that translate into Project classes
    def resolve_project(self, args, context, info):
        return Project(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

    def resolve_sample(self, args, context, info):
        return Sample(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', fmaBodySite='head')

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
