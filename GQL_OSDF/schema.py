import graphene
from graphene import relay
from models import Study, get_study

stu = get_study()

class Query(graphene.ObjectType): # grab everything at once

    study = graphene.Field(Study, facets=graphene.List(graphene.String), fields=graphene.List(graphene.String), fr=graphene.Int(description='j'), size=graphene.Int(description='size'), sort=graphene.String(description='j'))#, from=graphene.Int(), size=graphene.Int(), sort=graphene.String())
    node = relay.Node.Field() # get single Node if needed

    # Each resolver will return all the relevant nodes per model
    def resolve_project(self, args, context, info):
        return pro #get_project()

    def resolve_study(self, args, context, info):
        if args['facets'][1] == 'hi2':
            return Study(ID=['hi'], subtype=['hi'], center=['hi'], contact=['hi'], name=['hi'], description=['hi'], partOf=['hi'])

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
