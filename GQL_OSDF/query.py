import urllib2, re, json
from py2neo import Graph

# The base_query is the query to prepend all queries. The idea is to traverse
# the graph entirely and use filters to return a subset of the total traversal. 
base_query = "MATCH (pro:Project)<-[:PART_OF]-(stu:Study)<-[:PARTICIPATES_IN]-(sub:Subject)<-[:BY]-(vis:Visit)<-[:COLLECTED_DURING]-(sam:Sample)<-[:PREPARED_FROM]-(pf)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) WHERE "

tstr = '{"op":"and","content":[{"op":"AND","content":[{"op":"in","content":{"field":"cases.Project.name","value":["Human Microbiome Project (HMP)"]}},{"op":"OR","content":[{"op":"=","content":{"field":"cases.Sample.fma_body_site","value":"right cubital fossa [FMA:39849]"}},{"op":"=","content":{"field":"cases.SampleFmabodysite","value":"Gingiva [FMA:59762]"}}]}]}]}'
tstr2= '{"op":"and","content":[{"op":"in","content":{"field":"cases.Project.name","value":["Human Microbiome Project (HMP)"]}}]}'

# This is a recursive function originally used to traverse and find the depth 
# of nested JSON. Now used to traverse the op/filters query from GDC and 
# ultimately aims to provide input to build the WHERE clause of a Cypher query. 
def get_depth(x, arr):
    if type(x) is dict and x:
        if 'op' in x:
            arr.append(x['op'])
        if 'field' in x:
            left = x['field']
            right = x['value']
            if type(x['value']) is list:
                l = x['value']
                l = ["'{0}'".format(element) for element in l] # need to add quotes around each element to make Cypher happy
                right = ",".join(l)
            else:
                right = "'{0}'".format(right) # again, quotes for Cypher
            arr.append(left)
            arr.append(right)
        return max(get_depth(x[a], arr) for a in x)
    if type(x) is list and x: 
        return max(get_depth(a, arr) for a in x)
    return arr # give the array back after traversal is complete

comp_ops = ["=",">",">=","<","<=","!=","EXCLUDE","IN","IS","NOT"]
comps = set(comp_ops)
skip_me = set()

def build_where(inp):
    for x in reversed(range(0,len(inp))): # makes more sense to build up than it is to build down
        
        if x in skip_me: # pass over elements we know are already consumed
            pass
        elif inp[x-2] in comps: # case to build comparison statement
            print "%s %s %s" % (inp[x-1],inp[x-2],inp[x])
            skip_me.update(range(x-2,x))
        else: # process the overarching AND/OR of the WHERE
            print x
        
fs = json.loads(tstr) # from string to json
arr = []
where = get_depth(fs, arr)
build_where(where)

# Need a function for building the RETURN clause and then put together the final query

#print fs['content'][0]['content'][0]
#print fs['content'][0]['content'][1]
#print fs['content'][0]['content'][1]['content'][0]['content']['field']
#print fs['content'][0]['content'][1]['content'][0]['op']
#print fs['content'][0]['content'][1]['content'][0]['content']['value']