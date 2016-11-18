import graphene
from models import Pagination, CaseHits, Aggregations, get_buckets, get_case_hits, get_pagination

# Can preload counts. These aggregations can remain stagnant so don't need to update
# based on filters as these are used to give a total count of the data.
proName = get_buckets("project.name","no","")
samFMA = get_buckets("sample.fma_body_site","no","")
samGLN = get_buckets("sample.geo_loc_name","no","")
subGender = get_buckets("subject.gender","no","")

class Query(graphene.ObjectType):

    pagination = graphene.Field(Pagination, cy=graphene.String(description='Cypher WHERE parameters'), s=graphene.Int(description='size of subset to return'), f=graphene.Int(description='what position of the sort to start at'))
    hits = graphene.List(CaseHits, cy=graphene.String(description='Cypher WHERE parameters'), s=graphene.Int(description='size of subset to return'), o=graphene.String(description='what to sort by'), f=graphene.Int(description='what position of the sort to start at'))
    aggregations = graphene.Field(Aggregations)

    def resolve_pagination(self, args, context, info):
        cy = args['cy'].replace("|",'"')
        return get_pagination(cy,args['s'],args['f'],'c')

    def resolve_hits(self, args, context, info):
        cy = args['cy'].replace("|",'"') # handle quotes for GQL
        o = args['o'].replace("case_id","Sample.id") # lose the portal ordering syntax
        o = o.replace(".raw","")
        if args['cy'] == "":
            return get_case_hits(args['s'],"Sample.id:asc",args['f'],"")
        else:
            return get_case_hits(args['s'],o,args['f'],cy)

    def resolve_aggregations(self, args, context, info):
        return Aggregations(Project_name=proName, Sample_fmabodysite=samFMA, Subject_gender=subGender,
            Sample_geolocname=samGLN)
        
ac_schema = graphene.Schema(query=Query)