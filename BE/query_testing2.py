import urllib2, re, json
from multiprocessing import Process, Queue, Pool

# The match var is the base query to prepend all queries. The idea is to traverse
# the graph entirely and use filters to return a subset of the total traversal. 
match = "MATCH (Project)<-[:PART_OF]-(Study)<-[:PARTICIPATES_IN]-(Subject)<-[:BY]-(Visit)<-[:COLLECTED_DURING]-(Sample)<-[:PREPARED_FROM]-(pf)<-[:SEQUENCED_FROM]-(sf)<-[:COMPUTED_FROM]-(cf) WHERE"

tstr = '{"query":"cases.ProjectName = Human Microbiome Project (HMP)"}'
tstr2 = '{"op":"and","content":[{"op":"in","content":{"field":"cases.Project.name","value":["Human Microbiome Project (HMP)"]}}]}'
tstr3 = '{"query":"cases.ProjectName = \"Human Microbiome Project (HMP)\""}'
tstr4 = '{"query":"cases.ProjectName = \"Human Microbiome Project (HMP)\" AND cases.SubjectGender = male"}'
tstr5 = '{"query":"(cases.SampleFmabodysite = posterior_fornix OR cases.SubjectGender = male)"}'

comp_ops = ["=",">",">=","<","<=","!=","EXCLUDE","IN","in","IS","NOT"] # distinguishing factor from the next is "in" which is utilized in facet
comp_ops2 = ["AND","OR","=",">",">=","<","<=","!=","EXCLUDE","IN","IS","NOT", "and", "or"] # separate group to delineate when to combine left/right halves of string
comps = set(comp_ops)
comps2 = set(comp_ops2)

# This is a recursive function originally used to traverse and find the depth 
# of nested JSON. Now used to traverse the op/filters query from GDC and 
# ultimately aims to provide input to build the WHERE clause of a Cypher query. 
# Accepts input of json.loads parsed GDC portal query input and an empty array.
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

# Fxn to build Cypher based on facet search, accepts output from get_depth
def build_facet_where(inp): 
    facets = [] # going to build an array of all the facets params present
    lstr, rstr = ("" for i in range(2))
    for x in reversed(range(0,len(inp))):
        if "'" in inp[x]: # found the values to search for
            rstr = "[%s]" % (inp[x]) # add brackets for Cypher
        elif "." in inp[x]: # found the fields to search on
            lstr = inp[x]
        elif "in" == inp[x]: # found the comparison op, build the full string
            facets.append("%s in %s" % (lstr, rstr))
    return " AND ".join(facets) # send back Cypher-ready WHERE clause

# Note that body_site and fma_body_site are HMP and iHMP specific, respectively. If the 
# following return ends in "counts", then it is for a pie chart. The first two are for
# cases/files tabs and the last is for the total size. 
returns = {
    'cases': "RETURN Project.name, Project.subtype, Sample.body_site, Sample.id, Study.name",
    'files': "RETURN Project, sf, cf, Sample.id",
    'name': "RETURN Project.name as prop, count(Project.name) as counts",
    'name_detailed': "RETURN Project.name as prop, count(Project.name) as ccounts, (count(sf)+count(cf)) as dcounts, (SUM(toInt(sf.size))+SUM(toInt(cf.size))) as tot",
    'body_site': "RETURN Sample.body_site as prop, count(Sample.body_site) as counts",
    'body_site_detailed': "RETURN Sample.body_site as prop, count(Sample.body_site) as ccounts, (count(sf)+count(cf)) as dcounts, (SUM(toInt(sf.size))+SUM(toInt(cf.size))) as tot",    
    'fma_body_site': "RETURN Sample.fma_body_site as prop, count(Sample.fma_body_site) as counts",
    'study': "RETURN Study.name as prop, count(Study.name) as counts",
    'gender': "RETURN Subject.gender as prop, count(Subject.gender) as counts",
    'gender_detailed': "RETURN Subject.gender as prop, count(Subject.gender) as ccounts, (count(sf)+count(cf)) as dcounts, (SUM(toInt(sf.size))+SUM(toInt(cf.size))) as tot",
    'race': "RETURN Subject.race as prop, count(Subject.race) as counts",
    'format': "RETURN sf.format as prop, count(sf.format) as counts",
    'size': "RETURN (SUM(toInt(sf.size))+SUM(toInt(cf.size))) as tot",
    'pagination': "RETURN (count(sf)+count(cf)) AS tot"
}

# Final function needed to build the entirety of the Cypher query. Accepts the following:
# match = base MATCH query for Cypher
# whereFilters = filters string passed from GDC portal
# order = parameters to order results by (needed for pagination)
# start = index of sort to start at
# size = number of results to return
# rtype = return type, want to be able to hit this for both cases, files, and aggregation counts.
def build_cypher(match,whereFilters,order,start,size,rtype):
    arr = []
    q = json.loads(whereFilters) # parse filters input into JSON (yields hashes of arrays)
    w1 = get_depth(q, arr) # first step of building where clause is the array of individual comparison elements
    where = build_facet_where(w1)
    where = where.replace("cases.","") # trim the GDC syntax, hack until we refactor cases/files syntax
    where = where.replace("files.","")
    order = order.replace("cases.","")
    order = order.replace("files.","")
    retval1 = returns[rtype] # actual RETURN portion of statement
    if rtype in ["cases","files"]: # pagination handling needed for these returns
        order = order.split(":")
        retval2 = "ORDER BY %s %s SKIP %s LIMIT %s" % (order[0],order[1].upper(),start-1,size) 
        return "%s %s %s %s" % (match,where,retval1,retval2)
    else:
        return "%s %s %s" % (match,where,retval1)

# First iteration, handling all regex individually
regexForNotEqual = re.compile(r"<>\s([0-9]*[a-zA-Z_]+[a-zA-Z0-9_]*)\b") # only want to add quotes to anything that's not solely numbers
regexForEqual = re.compile(r"=\s([0-9]*[a-zA-Z_]+[a-zA-Z0-9_]*)\b") 

def build_adv_cypher(match,whereFilters,order,start,size,rtype):
    where = whereFilters[10:len(whereFilters)-2] 
    where = where.replace("cases.ProjectName","Project.name")
    where = where.replace("cases.SubjectGender","Subject.gender")
    where = where.replace("cases.SampleFmabodysite","Sample.body_site")
    # Add quotes that FE missed
    if '=' in where:
        where = regexForEqual.sub(r'"\1"',where)
    if '<>' in where:
        where = regexForNotEqual.sub(r'"\1"',where)

    order = order.replace("cases.","")
    order = order.replace("files.","")
    retval1 = returns[rtype] # actual RETURN portion of statement
    if rtype in ["cases","files"]: # pagination handling needed for these returns
        order = order.split(":")
        retval2 = "ORDER BY %s %s SKIP %s LIMIT %s" % (order[0],order[1].upper(),start-1,size) 
        return "%s %s %s %s" % (match,where,retval1,retval2)
    else:
        return "%s %s %s" % (match,where,retval1)

tstr1 = 'cases.SubjectGender != female AND cases.ProjectName IN [iHMP,    "Human Microbiome Project (HMP)"] OR gender IN [male,female,Chron\'s]'
#tstr1 = 'cases.SubjectGender != female OR gender IN [male,female,\'unknown\']'
#tstr1 = regexForNotEqual.sub(r'<> "\1"',tstr1)
#print regexForEqual.sub(r'"\1"',tstr1)
#print s

regexForIn = re.compile(r"(\[[a-zA-Z\'\"\s\,\(\)]+\])") # catch anything that should be in a list
x = re.findall(regexForIn,tstr1)
listDict = {}
for z in x:
    original = z
    z = z.replace('[','')
    z = z.replace(']','')
    j = z.split(',')
    newL = []
    for y in j:
        if '"' in y:
            newL.append(y)
        else:
            newL.append('"%s"' % (y))
    z = ",".join(newL)
    new = "[%s]" % (z)
    listDict[original] = new

for k,v in listDict.iteritems():
    tstr1 = tstr1.replace(k,v)

print tstr1