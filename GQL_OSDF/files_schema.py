import graphene
from graphene import relay
from models import Project, AllFiles, get_proj_data, get_files

# Unlike the others, don't want to preload here. Load on call from each unique sample ID

class Query(graphene.ObjectType):

    project = graphene.Field(Project, id=graphene.String(description='Sample ID to query on'))
    files = graphene.Field(AllFiles, id=graphene.String(description='Sample ID to query on'))
    caseId = graphene.String(name="case_id", id=graphene.String(description='Sample ID to query on'))
    submitterId = graphene.String(name="submitter_id") # dummy value returned, accommodate GDC

    def resolve_project(self, args, context, info):
        return Project(name="123",projectId="testing")#get_proj_data(args['id'])

    def resolve_files(self, args, context, info):
        return get_files(args['id'])

    def resolve_caseId(self, args, context, info):
        return args['id']

    def resolve_submitterId(self, args, context, info):
        return "null"

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
files_schema = graphene.Schema(query=Query)
