Project = {}
DNAPrep16s = {}
RawSeqSet16s = {}
Sample = {}
Study = {}
Subject = {}
TrimmedSeqSet16s = {}
Visit = {}

# Using py2neo 3 not 2
from py2neo import Graph
from pandas import DataFrame
import numpy
graph = Graph("http://localhost:7474/db/data/")

# Example ES query+result
#es_result = graph.run("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"iHMP\"}}}') YIELD node return node").data()
#print(dumps(es_result))

# Example normal Cypher query+result
# The below returns an array where each element is a node+properties requested
#reg_result = graph.data("MATCH (n:Project) RETURN n.name")

def get_project():
    result = graph.data("MATCH (n:Project) RETURN n")
    return '123j'#(_id='123j', nodeType='project', aclRead='ihmp', aclWrite='ihmp', subtype='hmp', name='Human Microbiome Project(HMP)', description='asdlfj')

#def get_dnaprep16s(character):
   

#def get_rawseqset16s(episode):


#def get_sample(id):
    

#def get_study(id):
    

#def get_subject(id):
    

#def get_trimmedseqset16s(id):


#def get_visit(id):
    
