import graphene
from graphene import relay
from models import Project, Study, Subject, Visit, Sample, get_project, get_study, \
    get_subject, get_visit, get_sample

# Graphene really lends itself to modifying schema via Django. Since Django+(ES+Neo4j) isn't 
# all that comaptible, just make smarter models as they should suffice for our current needs.

class Query(graphene.ObjectType): # grab everything at once

    project = graphene.Field(Project)
    study = graphene.Field(Study)
    subject = graphene.Field(Subject)
    visit = graphene.Field(Visit)
    sample = graphene.Field(Sample)
    node = relay.Node.Field() # get single Node if needed

    # Each resolver will return all the relevant nodes per model
    def resolve_project(self, args, context, info):
        return get_project()

    def resolve_study(self, args, context, info):
        return get_study()

    def resolve_subject(self, args, context, info):
        return get_subject()

    def resolve_visit(self, args, context, info):
        return get_visit()

    def resolve_sample(self, args, context, info):
        return get_sample()     

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
