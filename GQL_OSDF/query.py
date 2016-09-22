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

es_site = "http://osdf-devel.igs.umaryland.edu:8123/nodes/query/ihmp"
data = json.dumps()

username = 
password = 

buffer = StringIO()
c = pycurl.Curl()
c.setopt(pycurl.URL, es_site)
c.setopt(pycurl.POST, 1)
c.setopt(pycurl.POSTFIELDS, data)
c.setopt(pycurl.USERPWD, '%s:%s' % (username, password))
c.setopt(pycurl.WRITEDATA, buffer)
c.perform()
c.close()

body = buffer.getValue()
print(body)

#def get_project(id):


#def get_dnaprep16s(character):
   

#def get_rawseqset16s(episode):


#def get_sample(id):
    

#def get_study(id):
    

#def get_subject(id):
    

#def get_trimmedseqset16s(id):


#def get_visit(id):
    
