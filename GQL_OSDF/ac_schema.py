import graphene
from graphene import relay
from models import Pagination, CaseHits, Aggregations, get_buckets, get_case_hits

# Can preload counts. These aggregations can remain stagnant so don't need to update
# based on filters as these are used to give a total count of the data.
proName = get_buckets("Project.name","no","")
samFMA = get_buckets("Sample.body_site","no","")

class Query(graphene.ObjectType):

    pagination = graphene.Field(Pagination)
    hits = graphene.List(CaseHits, cy=graphene.String(description='Cypher WHERE parameters'), s=graphene.Int(description='size of subset to return'), o=graphene.String(description='what to sort by'), f=graphene.Int(description='what position of the sort to start at'))
    aggregations = graphene.Field(Aggregations)

    def resolve_pagination(self, args, context, info):
        return Pagination(count=25, sort="case_id.raw:asc", fromNum=1, page=1, total=25, pages=1, size=25)

    def resolve_hits(self, args, context, info):
        cy = args['cy'].replace("|",'"') # handle quotes for GQL
        o = args['o'].replace("case_id","Sample._id") # lose the portal ordering syntax
        if args['cy'] == "":
            return get_case_hits(20,"Sample._id:asc",1,"")
        else:
            return get_case_hits(args['s'],o,args['f'],cy)

    def resolve_aggregations(self, args, context, info):
        return Aggregations(Project_name=proName, Sample_fmabodysite=samFMA)
        
ac_schema = graphene.Schema(query=Query)