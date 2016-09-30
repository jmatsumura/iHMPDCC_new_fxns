import graphene
from graphene import relay
from models import SBucketCounter, SBucket, FileSize, get_buckets

# Can preload counts
proName = get_buckets("Project.name","yes")
samFMA = get_buckets("Sample.fma_body_site","yes")

class Query(graphene.ObjectType):

    SampleFmabodysite = graphene.Field(SBucketCounter)
    ProjectName = graphene.Field(SBucketCounter)
    fs = graphene.Field(FileSize)

    def resolve_SampleFmabodysite(self, args, context, info):
        return samFMA

    def resolve_ProjectName(self, args, context, info):
        return proName

    def resolve_fs(self, args, context, info):
        return FileSize(value=123456789)

# As noted above, going to hit Neo4j once and get everything then let GQL 
# do its magic client side to return the values that the user wants. 
sum_schema = graphene.Schema(query=Query)
