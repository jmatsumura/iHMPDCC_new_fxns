Project = {}
DNAPrep16s = {}
RawSeqSet16s = {}
Sample = {}
Study = {}
Subject = {}
TrimmedSeqSet16s = {}
Visit = {}

import pycurl, json
from StringIO import StringIO

es_site = 
data = json.dumps({"from": 0, "size": 1, "query":{"term":{"node_type":"study"}}})

username = 
password = 

storage = StringIO()
c = pycurl.Curl()
c.setopt(c.URL, es_site)
c.setopt(c.POST, 1)
c.setopt(c.POSTFIELDS, data)
c.setopt(c.USERPWD, '%s:%s' % (username, password))
#c.setopt(c.VERBOSE, True)
c.setopt(c.WRITEFUNCTION, storage.write)
c.perform()
c.close()


#body = buffer.getValue()
#print(body)

#def get_project(id):


#def get_dnaprep16s(character):
   

#def get_rawseqset16s(episode):


#def get_sample(id):
    

#def get_study(id):
    

#def get_subject(id):
    

#def get_trimmedseqset16s(id):


#def get_visit(id):
    
