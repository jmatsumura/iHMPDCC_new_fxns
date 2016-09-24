import graphene
from graphene import relay
from models import Project, Study, Sample

schema = graphene.Schema()

# Graphene really lends itself to Schema+Django. For now, just
# make smarter models as they should suffice for our purposes. 

class Query(graphene.ObjectType): # grab everything at once

    project = graphene.Field(Project)
    study = graphene.Field(Study)
    sample = graphene.Field(Sample)
    node = relay.Node.Field() # get single Node if needed

    # Each resolver will return all the relevant nodes per model
    def resolve_project(self, args, context, info):
        #return get_project()
        return Project(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

    def resolve_study(self, args, context, info):
        #return get_study()
        return Study(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='1', center='2', contact='yes', name='no', description='maybe', projects=[])
        
    def resolve_sample(self, args, context, info):
        #return get_sample()
        return Sample(id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', fmaBodySite='head', studies=[])       

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema.query = Query
