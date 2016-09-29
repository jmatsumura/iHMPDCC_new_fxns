import graphene
from graphene import relay
from models import Pagination, Hits, Aggregations, BucketCounter, Bucket, Warnings, get_buckets

# Can preload counts
proName = get_buckets("Project.name")
samFMA = get_buckets("Sample.fma_body_site")

class Query(graphene.ObjectType):

    pagination = graphene.Field(Pagination)
    hits = graphene.List(graphene.String)
    aggregations = graphene.Field(Aggregations)

    def resolve_pagination(self, args, context, info):
        return Pagination(count=0, sort="case_id.raw:asc", fromNum=1, page=1, total=3, pages=1, size=0)

    def resolve_hits(self, args, context, info):
        el = []
        return el

    def resolve_aggregations(self, args, context, info):
        return Aggregations(Project_name=proName, Sample_fmabodysite=samFMA)
        
ac_schema = graphene.Schema(query=Query)