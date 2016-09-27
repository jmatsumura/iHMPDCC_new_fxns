import graphene
from graphene import relay
from models import Project, Study, Subject, Visit, Sample, DNAPrep16s, RawSeqSet16s, TrimmedSeqSet16s, \
    get_project, get_study, get_subject, get_visit, get_sample, get_dnaprep16s, get_rawseqset16s, get_trimmedseqset16s

# Graphene really lends itself to modifying schema via Django. Since Django+(ES+Neo4j) isn't 
# all that comaptible, just make smarter models as they should suffice for our current needs.

pro = get_project() # can load everything right off the bat (at server initialization) and speed up results
#stu = get_study()
#sub = get_subject()
#vis = get_visit()
#sam = get_sample()
#prep16s = get_dnaprep16s()
#raw16s = get_rawseqset16s()
#trimmed16s = get_trimmedseqset16s()

class Query(graphene.ObjectType): # grab everything at once

    project = graphene.Field(Project)
    study = graphene.Field(Study)
    subject = graphene.Field(Subject)
    visit = graphene.Field(Visit)
    sample = graphene.Field(Sample)
    prep16s = graphene.Field(DNAPrep16s)
    raw16s = graphene.Field(RawSeqSet16s)
    trimmed16s = graphene.Field(TrimmedSeqSet16s)

    node = relay.Node.Field() # get single Node if needed

    # Each resolver will return all the relevant nodes per model
    def resolve_project(self, args, context, info):
        return pro #get_project()

    def resolve_study(self, args, context, info):
        return stu #get_study()

    def resolve_subject(self, args, context, info):
        return sub #get_subject()

    def resolve_visit(self, args, context, info):
        return vis #get_visit()

    def resolve_sample(self, args, context, info):
        return sam #get_sample()
    
    def resolve_prep16s(self, args, context, info):
        return prep16s #get_dnaprep16s() 

    def resolve_raw16s(self, args, context, info):
        return raw16s #get_rawseqset16s()

    def resolve_trimmed16s(self, args, context, info):
        return trimmed16s #get_trimmedseqset16s() 

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
schema = graphene.Schema(query=Query)
