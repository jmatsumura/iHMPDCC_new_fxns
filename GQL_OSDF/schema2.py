import graphene
from graphene import relay
from models import Project

class Query(graphene.ObjectType):

    def pp():
        return Project
    pp()
    #project = graphene.Field(Project)

    #def resolve_project(self, args, context, info):
        #return Project
        
schema2 = graphene.Schema(query=Query)