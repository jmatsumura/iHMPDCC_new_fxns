# Quick syntax check to make sure the general approach for how this 
# should work is accounted for. 

import graphene

class Defaults(graphene.Interface):
   
    _id = graphene.ID()
    nodeType = graphene.String()
    aclRead = graphene.String()
    aclWrite = graphene.String()

class Project(graphene.ObjectType):

    class Meta:
        interfaces = (Defaults, )

    subtype = graphene.String()
    name = graphene.String()
    description = graphene.String()

class QueryProject(graphene.ObjectType):
    
    project = graphene.Field(Project)

    def resolve_project(self, args, context, info):
        return Project(_id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

schema_project = graphene.Schema(query=QueryProject)

query='''
    query something {
        project {
            Id
            nodeType
            aclRead
            aclWrite
            subtype
            name
            description
        }
    }
'''

def test_query():
    result = schema_project.execute(query)
    print(result.errors)
    assert not result.errors
    assert result.data == {
        'project': {
            'id': '123j',
            'nodeType': 'project',
            'aclRead': 'ihmp',
            'aclWrite': 'ihmp',
            'subtype': 'hmp',
            'name': 'Human Microbiome Project(HMP)',
            'description': 'asdlfj',
        }
    }

if __name__ == '__main__':
    result = schema_project.execute(query)
    print(result.data)