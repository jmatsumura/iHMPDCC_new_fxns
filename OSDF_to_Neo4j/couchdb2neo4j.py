# CREDIT TO necaris for the base script ~ https://gist.github.com/necaris/5604018
#
# Script to migrate OSDF CouchDB into Neo4j. This will collapse nodes into 
# Subject, Sample, and File nodes. 
# 
# Subject houses project, study, subject_attribute, and subject
#
# Sample houses visit, visit_attribute, sample, and sample_attribute
#
# File is the prep node as well as the file
#
#-*-coding: utf-8-*-

import time,sys,argparse,requests
from py2neo import Graph
from accs_for_couchdb2neo4j import fma_free_body_site_dict, study_name_dict

try:
    import simplejson as json
except ImportError:
    import json

def _print_error(message):
    """
    Print a message to stderr, with a newline.
    """
    sys.stderr.write(str(message) + "\n")
    sys.stderr.flush()


def _all_docs_by_page(db_url, page_size=10):
    """
    Helper function to request documents from CouchDB in batches ("pages") for
    efficiency, but present them as a stream.
    """
    # Tell CouchDB we only want a page worth of documents at a time, and that
    # we want the document content as well as the metadata
    view_arguments = {'limit': page_size, 'include_docs': "true"}

    # Keep track of the last key we've seen
    last_key = None

    while True:
        response = requests.get(db_url + "/_all_docs", params=view_arguments)

        # If there's been an error, stop looping
        if response.status_code != 200:
            _print_error("Error from DB: " + str(response.content))
            break

        # Parse the results as JSON. If there's an error, stop looping
        try:
            results = json.loads(response.content)
        except:
            _print_error("Unable to parse JSON: " + str(response.content))
            break

        # If there's no more data to read, stop looping
        if 'rows' not in results or not results['rows']:
            break

        # Otherwise, keep yielding results
        for r in results['rows']:
            last_key = r['key']
            yield r

        # Update the view arguments with the last key we've seen, so that we
        # can step forward properly by page. (Of course, we actually need a key
        # that is just _after_ the last one we've seen, so tack on a high
        # Unicode character).
        # Note that CouchDB requires keys to be encoded as JSON
        last_key = last_key + u'\xff'
        view_arguments.update(startkey=json.dumps(last_key))

# All of these _build*_doc functions take in a particular "File" node (which)
# means anything below the "Prep" nodes and build a document containing all 
# the information along the particular path to get to that node. Each will
# have a new top of the structure called "File" where directly below this 
# location will contain all the relevant data for that particular node. 
# Everything else even with this level will be all the information contained
# at prep and above. This will result in a heavily DEnormalized dataset.
#
# The arguments are the entire set of nodes and the particular node that is the
# file representative. 

def _build_16s_raw_seq_set_doc(all_nodes_dict,node):

    doc = {}
    doc['main'] = node['doc']

    doc['prep'] = _find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['main']['linkage']['sequenced_from'])

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_16s_trimmed_seq_set_doc(all_nodes_dict,node):

    doc = {}
    doc['main'] = node['doc']

    doc['16s_raw_seq_set'] = _find_upstream_node(all_nodes_dict['16s_raw_seq_set'],'16s_raw_seq_set',doc['main']['linkage']['computed_from'])
    doc['prep'] = _find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['16s_raw_seq_set']['linkage']['sequenced_from'])

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_abundance_matrix_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2)) # can be many here

    doc['main'] = node['doc']

    link = _refine_link(doc['main']['linkage']['computed_from'])

    # Notice that this IF precedes a second set of ELSE/IF statements, that is because
    # if this is an abundance_matrix derived from an abundance_matrix, we still build the
    # upstream structure in the same manner either way.
    if link in all_nodes_dict['abundance_matrix']:
        doc['abundance_matrix'] = _find_upstream_node(all_nodes_dict['abundance_matrix'],'abundance_matrix',link)
        # We now need to reset the link to be the other abundance_matrix
        link = _refine_link(doc['abundance_matrix']['linkage']['computed_from'])

    # process the middle pathway
    if link in all_nodes_dict['16s_trimmed_seq_set']:
        doc['16s_trimmed_seq_set'] = _find_upstream_node(all_nodes_dict['16s_trimmed_seq_set'],'16s_trimmed_seq_set',link)
        doc['16s_raw_seq_set'] = _find_upstream_node(all_nodes_dict['16s_raw_seq_set'],'16s_raw_seq_set',doc['16s_trimmed_seq_set']['linkage']['computed_from'])
        doc['prep'] = _find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['16s_raw_seq_set']['linkage']['sequenced_from'])

    # process the left pathway
    elif (
        link in all_nodes_dict['microb_transcriptomics_raw_seq_set'] 
        or link in all_nodes_dict['host_transcriptomics_raw_seq_set'] 
        or link in all_nodes_dict['wgs_raw_seq_set'] 
        or link in all_nodes_dict['host_wgs_raw_seq_set'] 
        ):
        if link in all_nodes_dict['microb_transcriptomics_raw_seq_set']:
            which_upstream = 'microb_transcriptomics_raw_seq_set'
        elif link in all_nodes_dict['host_transcriptomics_raw_seq_set']:
            which_upstream = 'host_transcriptomics_raw_seq_set'
        elif link in all_nodes_dict['wgs_raw_seq_set']:
            which_upstream = 'wgs_raw_seq_set'
        elif link in all_nodes_dict['host_wgs_raw_seq_set']:
            which_upstream = 'host_wgs_raw_seq_set'

        doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
        link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])

        if link in all_nodes_dict['wgs_dna_prep']:
            which_prep = 'wgs_dna_prep'
        elif link in all_nodes_dict['host_seq_prep']:
            which_prep = 'host_seq_prep'
        else:
            print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

        doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    # process the right pathway
    elif (
        link in all_nodes_dict['proteome']
        or link in all_nodes_dict['metabolome']
        or link in all_nodes_dict['lipidome']
        or link in all_nodes_dict['cytokine']
        ):
        if link in all_nodes_dict['proteome']:
            which_upstream = 'proteome'
        elif link in all_nodes_dict['metabolome']:
            which_upstream = 'metabolome'
        elif link in all_nodes_dict['lipidome']:
            which_upstream = 'lipidome'
        elif link in all_nodes_dict['cytokine']:
            which_upstream = 'cytokine'

        doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
        link = _refine_link(doc[which_upstream]['linkage']['derived_from'])

        if link in all_nodes_dict['microb_assay_prep']:
            which_prep = 'microb_assay_prep'
        elif link in all_nodes_dict['host_assay_prep']:
            which_prep = 'host_assay_prep'
        else:
            print("Made it here, so an ~ome node is missing an upstream ID of {0}.".format(link))

        doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_omes_doc(all_nodes_dict,node):

    doc = {}
    which_prep = "" # can be microb or host

    doc['main'] = node['doc']

    link = _refine_link(doc['main']['linkage']['derived_from'])

    if link in all_nodes_dict['microb_assay_prep']:
        which_prep = 'microb_assay_prep'
    elif link in all_nodes_dict['host_assay_prep']:
        which_prep = 'host_assay_prep'
    else:
        print("Made it here, so an ~ome node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_wgs_transcriptomics_doc(all_nodes_dict,node):

    doc = {}
    which_prep = "" # can be wgs_dna or host_seq

    doc['main'] = node['doc']

    link = _refine_link(doc['main']['linkage']['sequenced_from'])

    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_wgs_assembled_or_viral_seq_set_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc'] 

    link = _refine_link(doc['main']['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])
    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_annotation_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc'] 

    link = _refine_link(doc['main']['linkage']['computed_from'])

    if link in all_nodes_dict['viral_seq_set']:
        which_upstream = 'viral_seq_set'
    elif link in all_nodes_dict['wgs_assembled_seq_set']:
        which_upstream = 'wgs_assembled_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])

    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_clustered_seq_set_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc'] 

    doc['annotation'] = _find_upstream_node(all_nodes_dict['annotation'],'annotation',doc['main']['linkage']['computed_from'])
    link = _refine_link(doc['annotation']['linkage']['computed_from'])

    if link in all_nodes_dict['viral_seq_set']:
        which_upstream = 'viral_seq_set'
    elif link in all_nodes_dict['wgs_assembled_seq_set']:
        which_upstream = 'wgs_assembled_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])

    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

# This function takes in the dict of nodes from a particular node type, the name
# of this type of node, the ID specified by the linkage to isolate the node. 
# It returns the information of the particular upstream node. 
def _find_upstream_node(node_dict,node_name,link_id):
    
    # some test nodes have incorrect linkage styles.
    link_id = _refine_link(link_id)

    if link_id in node_dict:
        return node_dict[link_id]['doc']

    print("Made it here, so node type {0} with ID {1} is missing upstream.".format(node_name,link_id))

# This function collects sample-project nodes as these can consistently be 
# retrieved in a similar manner.
#
# Note a lack of *_attribute nodes. When real data for these is uploaded they
# will be tested and accounted for.
def _collect_sample_through_project(all_nodes_dict,doc):
    
    doc['sample'] = _find_upstream_node(all_nodes_dict['sample'],'sample',doc['prep']['linkage']['prepared_from'])
    doc['visit'] = _find_upstream_node(all_nodes_dict['visit'],'visit',doc['sample']['linkage']['collected_during'])
    doc['subject'] = _find_upstream_node(all_nodes_dict['subject'],'subject',doc['visit']['linkage']['by'])
    doc['study'] = _find_upstream_node(all_nodes_dict['study'],'study',doc['subject']['linkage']['participates_in'])
    doc['project'] = _find_upstream_node(all_nodes_dict['project'],'project',doc['study']['linkage']['part_of'])
    
    # Skip all the dummy data associated with the "Test Project"
    if doc['project']['id'] == '610a4911a5ca67de12cdc1e4b40018e1':
        return None
    else:
        return doc

# This simply reformats a ID specified from a linkage to ensure it's a string 
# and not a list. I haven't encountered any scenarios with multiple linkages 
# from a single node and do not think it is a problem. Accepts a an entity
# following a linkage like doc['linkage']['sequenced_from'|'derived_from']
def _refine_link(linkage):

    if type(linkage) is list:
        if linkage[0] == '3a51534abc6e1a5ee6d9cc86c400a5a3': # don't consider the demo project a study, ignore this ID
            return linkage[1]
        else:
            return linkage[0]
    else:
        return linkage

# Build indexes for the three node types and their IDs that guarantee UNIQUEness
def _build_constraint_index(node,prop,cy):
    cstr = "CREATE CONSTRAINT ON (x:{0}) ASSERT x.{1} IS UNIQUE".format(node,prop)
    cy.run(cstr)

# Build indexes for searching on all the props that aren't ID. Takes which node
# to build all indexes on as well as the Neo4j connection.
def _build_all_indexes(node,cy):
    result = cy.run("MATCH (n:{0}) WITH DISTINCT keys(n) AS keys UNWIND keys AS keyslisting WITH DISTINCT keyslisting AS allfields RETURN allfields".format(node))
    for x in result:
        prop = x['allfields']
        if prop != 'id':
            cy.run("CREATE INDEX ON :{0}(`{1}`)".format(node,prop))

# Escape quotes to keep Cypher happy
def _mod_quotes(val):

    if isinstance(val, list):
        for x in val:
            if x in fma_free_body_site_dict:
                x = fma_free_body_site_dict[x]
            x = x.replace("'","\\'")
            x = x.replace('"','\\"')
            
    else:
        if val in fma_free_body_site_dict:
            val = fma_free_body_site_dict[val]
        val = val.replace("'","\\'")
        val = val.replace('"','\\"')

    return val

# Function to traverse the nested JSON documents from CouchDB and return
# a flattened set of properties specific to the particular node. 
def _traverse_document(doc,focal_node):

    key_prefix = "" # for nodes embedded into other nodes, use this prefix to prepend their keys like project_name
    props = [] # list of all the properties to be added
    tags = [] # list of tags to be attached to the ID
    doc_id = "" # keep track of the ID for this particular doc.  

    if focal_node not in ['subject','sample','main']: # main is equivalent to file since a single doc represents a single file
        key_prefix = "{0}_".format(focal_node)

    for key,val in doc[focal_node].items():
        if key == 'linkage' or not val: # document itself contains all linkage info already
            continue

        if isinstance(val, int) or isinstance(val, float):
            props.append('`{0}{1}`:{2}'.format(key_prefix,key,val))
        elif isinstance(val, list): # lists should be urls, contacts, and tags
            for j in range(0,len(val)): 
                
                if key == 'tags':
                    tags.append(val)

                elif key == 'contact':
                    email = ""
                    for vals in val: # try find an email
                        if '@' in vals:
                            email = vals
                            break
                    if email:
                        props.append('`{0}contact`:"{1}"'.format(key_prefix,email))
                        break
                    else:
                        props.append('`{0}contact`:"{1}"'.format(key_prefix,val[j]))
                        break

                else:
                    endpoint = val[j].split(':')[0]
                    props.append('`{0}{1}`:"{2}"'.format(key_prefix,endpoint,val[j]))
        else:
            val = _mod_quotes(val)
            props.append('`{0}{1}`:"{2}"'.format(key_prefix,key,val))

        if key == "id":
            doc_id = val

    props_str = (',').join(props)

    return {'id':doc_id,'tag_list':tags,'prop_str':props_str}

# Function to insert into Neo4j. Takes in Neo4j connection and a document.
def _insert_into_neo4j(cy,doc):
    if doc is not None:

        # Grab just the properties of the file and prep nodes
        file_info = _traverse_document(doc,'main')
        prep_info = _traverse_document(doc,'prep')
        all_tags = []

        props = "{0},{1}".format(file_info['prop_str'],prep_info['prop_str'])
        cy.run("CREATE (node:file {{ {0} }})".format(props))

        sample_info = _traverse_document(doc,'sample')
        visit_info = _traverse_document(doc,'visit')

        props = "{0},{1}".format(sample_info['prop_str'],visit_info['prop_str'])
        cy.run("MERGE (node:sample {{ {0} }})".format(props))

        subject_info = _traverse_document(doc,'subject')
        study_info = _traverse_document(doc,'study')
        project_info = _traverse_document(doc,'project')
        props = "{0},{1},{2}".format(subject_info['prop_str'],study_info['prop_str'],project_info['prop_str'])
        cy.run("MERGE (node:subject {{ {0} }})".format(props))

        cy.run("MATCH (n1:subject{{id:'{0}'}}),(n2:sample{{id:'{1}'}}) MERGE (n1)<-[:extracted_from]-(n2)".format(subject_info['id'],sample_info['id']))
        cy.run("MATCH (n2:sample{{id:'{0}'}}),(n3:file{{id:'{1}'}}) MERGE (n2)<-[:derived_from]-(n3)".format(sample_info['id'],file_info['id']))

'''
        all_tags += file_info['tag_list']
        all_tags += prep_info['tag_list']
        all_tags += sample_info['tag_list']
        all_tags += visit_info['tag_list']
        all_tags += subject_info['tag_list']
        all_tags += study_info['tag_list']
        all_tags += project_info['tag_list']
        unique_tags = set(all_tags)

        for tag in unique_tags:
            if ":" in tag:
                tag = tag.split(':',1)[1] # don't trim URLs and the like (e.g. http:)
                tag = tag.strip()
            if tag: # if there's something there, attach
                cy.run("MATCH (n1:file{{id:'{0}'}}) MERGE (tag{{term:'{1}'}})<-[:has_tag]-(n1)".format(file_info['id'],tag))
'''

# Takes a dictionary from the OSDF doc and builds a list of the keys that are
# irrelevant.
def _delete_keys_from_dict(doc_dict):
    delete_us = []

    for key,val in doc_dict.items():
        if not val or not key:
            delete_us.append(key)

        # Unfortunately... have to check for keys comprised of blanks paces
        if len(key.replace(' ','')) == 0:
            delete_us.append(key)

    for empty in delete_us:
        del doc_dict[empty]

    return doc_dict


if __name__ == '__main__':

    # Set up an ArgumentParser to read the command-line
    parser = argparse.ArgumentParser(
        description="Dump documents out of CouchDB to the filesystem")

    parser.add_argument(
        '--db', type=str,
        help="The CouchDB database URL from which to load data")

    parser.add_argument(
        "--page_size", type=int, default=1000,
        help="How many documents to request from CouchDB in each batch.")

    parser.add_argument(
        "--neo4j_password", default="neo4j",
        help="The password for Neo4j")

    args = parser.parse_args()

    cy = Graph(password = args.neo4j_password)

    _build_constraint_index('subject','id',cy)
    _build_constraint_index('sample','id',cy)
    _build_constraint_index('file','id',cy)
    _build_constraint_index('tag','term',cy)

    # Now just loop through and create documents. I like counters, so there's
    # one to tell me how much has been done. I also like timers, so there's one
    # of them too.
    counter = 1
    start_time = time.time()

    # Dictionaries for each nodes where it goes like {project{id{couch_db_doc}}} so that
    # it is fast to look up IDs when traversing upstream.
    nodes = {
        'project': {},
        'study': {},
        'subject': {},
        'subject_attribute': {},
        'visit': {},
        'visit_attribute': {},
        'sample': {},
        'sample_attribute': {},
        'wgs_dna_prep': {},
        'host_seq_prep': {},
        'wgs_raw_seq_set': {},
        'wgs_raw_seq_set_private': {},
        'host_wgs_raw_seq_set': {},
        'microb_transcriptomics_raw_seq_set': {},
        'host_transcriptomics_raw_seq_set': {},
        'wgs_assembled_seq_set': {},
        'viral_seq_set': {},
        'annotation': {},
        'clustered_seq_set': {},
        '16s_dna_prep': {},
        '16s_raw_seq_set': {},
        '16s_trimmed_seq_set': {},
        'microb_assay_prep': {},
        'host_assay_prep': {},
        'proteome': {},
        'metabolome': {},
        'lipidome': {},
        'cytokine': {},
        'abundance_matrix': {}
    }

    files_only = {
        'wgs_raw_seq_set',
        'wgs_raw_seq_set_private',
        'host_wgs_raw_seq_set',
        'microb_transcriptomics_raw_seq_set',
        'host_transcriptomics_raw_seq_set',
        'wgs_assembled_seq_set',
        'viral_seq_set',
        'annotation',
        'clustered_seq_set',
        '16s_dna_prep',
        '16s_raw_seq_set',
        '16s_trimmed_seq_set',
        'microb_assay_prep',
        'host_assay_prep',
        'proteome',
        'metabolome',
        'lipidome',
        'cytokine',
        'abundance_matrix'
    }

    for doc in _all_docs_by_page(args.db, args.page_size):
        # Assume we don't want design documents, since they're likely to be
        # already stored elsewhere (e.g. in version control)
        if doc['id'].startswith("_design"):
            continue
        elif doc['id'].endswith("_hist"):
            continue

        # Clean up the document a bit. We don't need everything stored in
        # CouchDB for this instance.
        if 'value' in doc:
            del doc['value']
        if 'key' in doc:
            del doc['key']
        if '_id' in doc['doc']:
            del doc['doc']['_id']
        if '_rev' in doc['doc']:
            del doc['doc']['_rev']
        if 'acl' in doc['doc']:
            del doc['doc']['acl']
        if 'ns' in doc['doc']:
            del doc['doc']['ns']
        if 'subset_of' in doc['doc']['linkage']:
            del doc['doc']['linkage']['subset_of']

        # Clean up all these empty values
        doc['doc'] = _delete_keys_from_dict(doc['doc'])
        if 'meta' in doc['doc']:

            # Private nodes should have some mock URL data in them
            if 'urls' in doc['doc']['meta']:
                if len(doc['doc']['meta']['urls'])==1 and doc['doc']['meta']['urls'][0]== "":
                    doc['doc']['meta']['urls'][0] = 'Private:Private Data ({0})'.format(doc['id'])

            doc['doc']['meta'] = _delete_keys_from_dict(doc['doc']['meta'])

            if 'mixs' in doc['doc']['meta']:
                doc['doc']['meta']['mixs'] = _delete_keys_from_dict(doc['doc']['meta']['mixs'])

            if 'mimarks' in doc['doc']['meta']:
                doc['doc']['meta']['mimarks'] = _delete_keys_from_dict(doc['doc']['meta']['mimarks'])

        # At this point we should have purged the document of all properties
        # that have no value attached to them.

        # Now move meta values a step outward and make them a base property instead of nested
        if 'meta' in doc['doc']:

            for key,val in doc['doc']['meta'].items():

                if isinstance(val,dict): # if a nested dict, extract

                    for ke,va in doc['doc']['meta'][key].items():

                        if isinstance(va,dict):
                            
                            for k,v in doc['doc']['meta'][key][ke].items(): 
                                if k and v:
                                    doc['doc'][k] = v

                        else:
                            if ke and va:
                                doc['doc'][ke] = va

                else:
                    if key and val:
                        doc['doc'][key] = val

            del doc['doc']['meta']

        doc['doc']['id'] = doc['id']

        # Fix the old syntax to make sure it reads 'attribute' and not just 'attr'
        if doc['doc']['node_type'].endswith("_attr"):
            doc['doc']['node_type'] = "{0}ibute".format(doc['doc']['node_type'])

        # Build a giant list of each node type
        if doc['doc']['node_type'] in nodes:
            nodes[doc['doc']['node_type']][doc['id']] = doc
        else:
            print("Warning, skipping node with type: {0}".format(doc['doc']['node_type']))

        # Catch any wonky edges at the document level, should only have one
        # although if these were laid out in a graph DB there could be multiple
        # nodes going to another like when samples are pooled.
        if 'linkage' in doc['doc']:
            if len(doc['doc']['linkage']) > 1:
                print("A document has multiple linkages! See:\n".format(doc['doc']))

        key = counter

        counter += 1
        sys.stderr.write(str(counter) + '\r')
        sys.stderr.flush()

    for key in nodes:

        if key in files_only:

            if key == "16s_raw_seq_set":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_16s_raw_seq_set_doc(nodes,nodes[key][id]))

            elif key == "16s_trimmed_seq_set":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_16s_trimmed_seq_set_doc(nodes,nodes[key][id]))

            elif key.endswith("ome") or key == "cytokine":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_omes_doc(nodes,nodes[key][id]))

            elif key == "abundance_matrix":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_abundance_matrix_doc(nodes,nodes[key][id]))

            elif (
                key == "wgs_raw_seq_set" or key == "wgs_raw_seq_set_private" 
                or key == "host_wgs_raw_seq_set" or key == "host_transcriptomics_raw_seq_set"
                or key == "microb_transcriptomics_raw_seq_set"
                ):
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_wgs_transcriptomics_doc(nodes,nodes[key][id]))

            elif key == "wgs_assembled_seq_set" or key == "viral_seq_set":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_wgs_assembled_or_viral_seq_set_doc(nodes,nodes[key][id]))

            elif key == "annotation":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_annotation_doc(nodes,nodes[key][id]))

            elif key == "clustered_seq_set":
                for id in nodes[key]:
                    _insert_into_neo4j(cy,_build_clustered_seq_set_doc(nodes,nodes[key][id]))

    # Here set some better syntax for the portal and override the original OSDF values
    cy.run('MATCH (n:subject) SET n.study_full_name=n.study_name')
    for old,new in study_name_dict.items():
        cy.run('MATCH (n:subject) WHERE n.study_name="{0}" SET n.study_name="{1}"'.format(old,new))
    cy.run("MATCH (PSS:subject) WHERE PSS.project_name = 'iHMP' SET PSS.project_name = 'Integrative Human Microbiome Project'")

    # Now build indexes on each unique property found in this newest data set
    _build_all_indexes('subject',cy)
    _build_all_indexes('sample',cy)
    _build_all_indexes('file',cy)

    # A little final message
    sys.stderr.write("Done! {0} documents in {1} seconds!\n".format(
        counter, time.time() - start_time))
