# CREDIT TO necaris for the base script ~ https://gist.github.com/necaris/5604018
#
# Script to migrate OSDF CouchDB into Couchbase. This will collapse nodes
# into single documents based on the File as the base.
#
#-*-coding: utf-8-*-
"""
Simple script to dump documents out of a CouchDB database and straight into
a Couchbase instance.
"""

import time,sys,argparse,requests

try:
    import simplejson as json
except ImportError:
    import json

from couchbase import Couchbase


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


if __name__ == '__main__':

    # Set up an ArgumentParser to read the command-line
    parser = argparse.ArgumentParser(
        description="Dump documents out of CouchDB to the filesystem")

    parser.add_argument(
        '--db', type=str,
        help="The CouchDB database URL from which to load data")

    parser.add_argument(
        "--page-size", type=int, default=1000,
        help="How many documents to request from CouchDB in each batch.")

    parser.add_argument(
        "--couchbase-host", default="127.0.0.1",
        help="The host for the Couchbase server")

    parser.add_argument(
        "--couchbase-bucket", default="default",
        help="The destination Couchbase bucket")

    parser.add_argument(
        "--couchbase-password", default="",
        help="The password for the destination bucket")

    args = parser.parse_args()

    # Create the Couchbase connection, and bail if it doesn't work
    cb = Couchbase.connect(host=args.couchbase_host,
                           bucket=args.couchbase_bucket,
                           password=args.couchbase_password)

    # Now just loop through and create documents. I like counters, so there's
    # one to tell me how much has been done. I also like timers, so there's one
    # of them too.
    counter = 1
    start_time = time.time()

    nodes = {
        'project': [],
        'study': [],
        'subject': [],
        'subject_attribute': [],
        'visit': [],
        'visit_attribute': [],
        'sample': [],
        'sample_attribute': [],
        'wgs_dna_prep': [],
        'host_seq_prep': [],
        'wgs_raw_seq_set': [],
        'wgs_raw_seq_set_private': [],
        'host_wgs_raw_seq_set': [],
        'microb_transcriptomics_raw_seq_set': [],
        'host_transcriptomics_raw_seq_set': [],
        'wgs_assembled_seq_set': [],
        'viral_seq_set': [],
        'annotation': [],
        'clustered_seq_set': [],
        '16s_dna_prep': [],
        '16s_raw_seq_set': [],
        '16s_trimmed_seq_set': [],
        'microb_assay_prep': [],
        'host_assay_prep': [],
        'proteome': [],
        'metabolome': [],
        'lipidome': [],
        'cytokine': [],
        'abundance_matrix': []
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

        doc['doc']['id'] = doc['id']

        # Fix the old syntax to make sure it reads 'attribute' and not just 'attr'
        if doc['doc']['node_type'].endswith("_attr"):
            doc['doc']['node_type'] = "{0}ibute".format(doc['doc']['node_type'])

        # Build a giant list of each node type
        if doc['doc']['node_type'] in nodes:
            nodes[doc['doc']['node_type']].append(doc['doc'])
        else:
            print("Warning, skipping node with type: {0}".format(doc['doc']['node_type']))

        # Catch any wonky edges at the document level, should only have one
        # although if these were laid out in a graph DB there could be multiple
        # nodes going to another like when samples are pooled.
        if len(doc['doc']['linkage']) > 1:
            print(doc['doc'])

        key = counter

        #cb.set(key, doc['doc'])

        counter += 1
        sys.stderr.write(str(counter) + '\r')
        sys.stderr.flush()

    # A little final message
    sys.stderr.write("Done! {0} documents in {1} seconds!\n".format(
        counter, time.time() - start_time))
