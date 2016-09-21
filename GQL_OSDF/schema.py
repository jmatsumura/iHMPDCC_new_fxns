import graphene

# Import from cypher.py the various Cypher queries to hit Neo4j and essentially
# return all the nodes present. 
#from .cypher import get_project

class Defaults(graphene.Interface):
   
    _id = graphene.ID()
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()

class Project(graphene.ObjectType):

    class Meta:
        interfaces = (Defaults, )

    subtype = graphene.String()
    name = graphene.String()
    description = graphene.String()

class Query(graphene.ObjectType):
    
    project = graphene.Field(Project)

    def resolve_project(self, args, context, info):
        return Project(_id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
