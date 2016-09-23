import graphene
from graphene import relay

# Import the various Cypher queries to hit Neo4j and build the schema
#from .neo4j_query import get_project

class Defaults(graphene.Interface):
   
    _id = graphene.ID()
    nodeType = graphene.String()

    # No reason the user needs to see these although they can be used to filter what is returned
    aclRead = graphene.String()
    aclWrite = graphene.String()

class Project(graphene.ObjectType):

    class Meta:
        interfaces = (Defaults, )

    subtype = graphene.String()
    name = graphene.String()
    description = graphene.String()

class Query(graphene.ObjectType):
    
    #node = relay.NodeField()
    project = graphene.Field(Project)

    def resolve_project(self, args, context, info):
        #return get_project()
        return Project(_id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
