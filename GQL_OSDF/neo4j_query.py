# Using py2neo 3 not 2
from py2neo import Graph
from pandas import DataFrame
from schema import Project, Study, Sample

graph = Graph("http://localhost:7474/db/data/")

# Example ES query+result
#print(graph.run("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"iHMP\"}}}') YIELD node return node").data())

# Example normal Cypher query+result
#print(graph.data("MATCH (n:Project) RETURN n.name"))

attr = "node_type"
val = "project"
test = "CALL ga.es.queryNode('{\"query\":{\"match\":{\"%s\":\"%s\"}}}') YIELD node return node" % (attr, val)

print(graph.data(test))

def get_project(): # retrieve a single project (done once a project is decided upon)
    #from schema import Project # need to figure out circular dependency work around
    result = graph.data(test)
    print 'dummy'#(_id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')


#def get_all_projects(): # retrieve all projects


#def get_dnaprep16s(character):
   

#def get_rawseqset16s(episode):


#def get_sample(id):
    

#def get_study(id):
    

#def get_subject(id):
    

#def get_trimmedseqset16s(id):


#def get_visit(id):
    
