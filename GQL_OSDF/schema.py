import graphene
from graphene import relay
from models import Project, Study, Sample, get_project

# Graphene really lends itself to modifying schema via Django. Since Django+(ES+Neo4j) isn't 
# all that comaptible, just make smarter models as they should suffice for our current needs.

class Query(graphene.ObjectType): # grab everything at once

    project = graphene.Field(Project)
    study = graphene.Field(Study)
    sample = graphene.Field(Sample)
    node = relay.Node.Field() # get single Node if needed

    # Each resolver will return all the relevant nodes per model
    def resolve_project(self, args, context, info):
        return get_project()
        #return Project(ID=['1','2'], nodeType=['project'], aclRead=['ihmp'], subtype=['1','2'], name=['test1','test2'], description=['asdlf'])

    def resolve_study(self, args, context, info):
        #return get_study()
        return Study(ID=['123j'], nodeType=['project'], aclRead=['ihmp'], subtype=['1'], center=['2'], contact=['yes'], name=['test1','test2'], description=['maybe'], partOf=['hi'])
        
    def resolve_sample(self, args, context, info):
        #return get_sample()
        return Sample(ID='123j', nodeType='project', aclRead='ihmp', fmaBodySite='head', collectedDuring=['1'])       

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
