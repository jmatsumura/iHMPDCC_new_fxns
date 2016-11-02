#!/usr/bin/python
#
# Contains accessories (1 function and some dicts) to convert from OSDF syntax to what will be loaded in Neo4j.

# Shared function across phases for modifying quotes in values.
# This function takes in a unicode value found in the couch dump and will replace quotes,
# either single or double, with literal quotes so that these can be passed to a Cypher 
# statement without escaping out early. May seem a bit round-about, but essentially 
# each unicode value needs to turn into a string in order to do replacement of quotes
# and must be sent back to unicode for Python processing into an eventual Cypher query.
def mod_quotes(val):
    if isinstance(val, unicode):
        val = val.encode('utf-8')
        val = val.replace("'",r"\'")
        val = val.replace('"',r'\"')
        val = val.decode('utf-8')
        # In order to search the DB as you would expect, convert number only strings to digits
        if val.isdigit():
            val = float(val) # float just in case
    return val

# This dictionary simply reformats aspects like capitalization for Neo4j. 
nodes = {
    'project': 'Case',
    'study': 'Case',
    'subject': 'Case',
    'subject_attr': 'Case',
    'subject_attribute': 'Case',
    'visit': 'Case',
    'visit_attr': 'Case',
    'visit_attribute': 'Case',
    'sample': 'Case',
    'sample_attr': 'Case',
    'sample_attribute': 'Case',
    'wgs_dna_prep': 'File',
    'host_seq_prep': 'File',
    'wgs_raw_seq_set': 'File',
    'wgs_raw_seq_set_private': 'File',
    'host_wgs_raw_seq_set': 'File',
    'microb_transcriptomics_raw_seq_set': 'File',
    'host_transcriptomics_raw_seq_set': 'File',
    'wgs_assembled_seq_set': 'File',
    'viral_seq_set': 'File',
    'annotation': 'File',
    'clustered_seq_set': 'File',
    '16s_dna_prep': 'File',
    '16s_raw_seq_set': 'File',
    '16s_trimmed_seq_set': 'File',
    'microb_assay_prep': 'File',
    'host_assay_prep': 'File',
    'proteome': 'File',
    'metabolome': 'File',
    'lipidome': 'File',
    'cytokine': 'File',
    'abundance_matrix': 'File',
    'tags': 'Tags',
    'mimarks': 'MIMARKS',
    'mixs': 'Mixs'
}

# These are all the different edge types present in the schema. 
edges = {
    'part_of': 'PART_OF',
    'subset_of': 'SUBSET_OF',
    'participates_in': 'PARTICIPATES_IN',
    'associated_with': 'ASSOCIATED_WITH',
    'by': 'BY',
    'collected_during': 'COLLECTED_DURING',
    'prepared_from': 'PREPARED_FROM',
    'sequenced_from': 'SEQUENCED_FROM',
    'derived_from': 'DERIVED_FROM',
    'computed_from': 'COMPUTED_FROM',
    'has_tag': 'HAS_TAG',
    'has_mimarks': 'HAS_MIMARKS',
    'has_mixs': 'HAS_MIXS'
}

definitive_edges = {
    'part_of': 'Case',
    'subset_of': 'Case',
    'participates_in': 'Case',
    'by': 'Case',
    'associated_with': 'Case',
    'collected_during': 'Case',
    'prepared_from': 'Case',
    'has_tag': 'Tags',
    'has_mimarks': 'MIMARKS',
    'has_mixs': 'Mixs'
}