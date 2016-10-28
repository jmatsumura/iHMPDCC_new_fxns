#!/usr/bin/python
#
# Contains dictionaries to convert from OSDF syntax to what will be loaded in Neo4j.

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
    'microb_assay_prep': 'FIle',
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