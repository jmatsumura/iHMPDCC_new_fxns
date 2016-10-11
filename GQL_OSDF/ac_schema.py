import graphene
from graphene import relay
from models import Pagination, CaseHits, Aggregations, get_buckets, get_case_hits

# Can preload counts
proName = get_buckets("Project.name","no")
samFMA = get_buckets("Sample.body_site","no")

class Query(graphene.ObjectType):

    pagination = graphene.Field(Pagination)
    hits = graphene.List(CaseHits)
    aggregations = graphene.Field(Aggregations)

    def resolve_pagination(self, args, context, info):
        return Pagination(count=25, sort="case_id.raw:asc", fromNum=1, page=1, total=25, pages=1, size=25)

    def resolve_hits(self, args, context, info):
        return get_case_hits()

    def resolve_aggregations(self, args, context, info):
        return Aggregations(ProjectName=proName, Sample_fmabodysite=samFMA)
        
ac_schema = graphene.Schema(query=Query)