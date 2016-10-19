#!/usr/bin/python
#
# Contains dictionaries to convert from OSDF syntax to what will be loaded in Neo4j.

# This dictionary simply reformats aspects like capitalization for Neo4j. 
nodes = {
    'project': 'Project',
    'study': 'Study',
    'subject': 'Subject',
    'subject_attr': 'Subject_Attr',
    'subject_attribute': 'Subject_Attr',
    'visit': 'Visit',
    'visit_attr': 'Visit_Attr',
    'visit_attribute': 'Visit_Attr',
    'sample': 'Sample',
    'sample_attr': 'Sample_Attr',
    'sample_attribute': 'Sample_Attr',
    'wgs_dna_prep': 'WGS_DNA_Prep',
    'host_seq_prep': 'Host_Seq_Prep',
    'wgs_raw_seq_set': 'WGS_Raw_Seq_Set',
    'wgs_raw_seq_set_private': 'WGS_Raw_Seq_Set_Private',
    'host_wgs_raw_seq_set': 'Host_WGS_Raw_Seq_Set',
    'microb_transcriptomics_raw_seq_set': 'Microb_Transcriptomics_Raw_Seq_Set',
    'host_transcriptomics_raw_seq_set': 'Host_Transcriptomics_Raw_Seq_Set',
    'wgs_assembled_seq_set': 'WGS_Assembled_Seq_Set',
    'viral_seq_set': 'Viral_Seq_Set',
    'annotation': 'Annotation',
    'clustered_seq_set': 'Clustered_Seq_set',
    '16s_dna_prep': '16S_DNA_Prep',
    '16s_raw_seq_set': '16S_Raw_Seq_Set',
    '16s_trimmed_seq_set': '16S_Trimmed_Seq_Set',
    'microb_assay_prep': 'Microb_Assay_Prep',
    'host_assay_prep': 'Host_Assay_Prep',
    'proteome': 'Proteome',
    'metabolome': 'Metabolome',
    'lipidome': 'Lipidome',
    'cytokine': 'Cytokine',
    'abundance_matrix': 'Abundance_Matrix',
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

# These are the edges with definitive endpoints (can only attach to one node).
# Thus, can use this information to subset and speed up the edge attachment in
# Neo4j. Others, like 'associated_with' or 'computed_from' can be derived from
# a variety of nodes so just have to do a generic search by ID to accommodate. 
definitive_edges = {
    'part_of': 'Project',
    'subset_of': 'Study',
    'participates_in': 'Study',
    'by': 'Subject',
    'collected_during': 'Visit',
    'prepared_from': 'Sample',
    'has_tag': 'Tags',
    'has_mimarks': 'MIMARKS',
    'has_mixs': 'Mixs'
}