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
graph = Graph("http://localhost:7474/db/data/")

# Example ES query+result
es_result = graph.run("CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"iHMP\"}}}') YIELD node, score return node, score").data()
print(es_result)

# Example normal Cypher query+result
reg_result = graph.run("MATCH (n:Project) WHERE n.name = 'iHMP' RETURN n").data()
print(reg_result)


#def get_project(id):


#def get_dnaprep16s(character):
   

#def get_rawseqset16s(episode):


#def get_sample(id):
    

#def get_study(id):
    

#def get_subject(id):
    

#def get_trimmedseqset16s(id):


#def get_visit(id):
    
