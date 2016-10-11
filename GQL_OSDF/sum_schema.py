import graphene
from graphene import relay
from models import SBucketCounter, FileSize, get_buckets, get_total_file_size

# Can preload counts
proName = get_buckets("Project.name","yes")
samFMA = get_buckets("Sample.body_site","yes")

class Query(graphene.ObjectType):

    SampleFmabodysite = graphene.Field(SBucketCounter)
    ProjectName = graphene.Field(SBucketCounter)
    fs = graphene.Field(FileSize, cy=graphene.String(description='Sample ID to query on'))

    def resolve_SampleFmabodysite(self, args, context, info):
        return samFMA

    def resolve_ProjectName(self, args, context, info):
        return proName

    def resolve_fs(self, args, context, info):
        # accept the pipes and convert to quotes again now that it's been passed across the URL
        cy = args['cy'].replace("|",'"') 
        return FileSize(value=get_total_file_size(cy))

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
sum_schema = graphene.Schema(query=Query)
