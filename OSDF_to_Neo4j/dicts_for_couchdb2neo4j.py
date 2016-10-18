#!/usr/bin/python
#
# Below are dictionaries to convert from OSDF syntax to what will be loaded
# in Neo4j. 
nodes = {
    'project': 'Project',
    'study': 'Study',
    'subject': 'Subject',
    'subject_attr': 'Subject_Attr',
    'visit': 'Visit',
    'visit_attr': 'Visit_Attr',
    'sample': 'Sample',
    'sample_attr': 'Sample_Attr',
    'wgs_dna_prep': 'WGS_DNA_Prep',
    'host_seq_prep': 'Host_Seq_Prep',
    'wgs_raw_seq_set': 'WGS_Raw_Seq_Set',
    'host_wgs_raw_seq_set': 'Host_WGS_Raw_Seq_Set',
    'microb_transcriptomics_raw_seq_set': 'Microb_Transcriptomics_Raw_Seq_Set',
    'host_transcriptomics_raw_seq_set': 'Host_Transcriptomics_Raw_Seq_Set',
    'wgs_assembled_seq_set': 'WGS_Assembled_Seq_Set',
    'viral_seq_set': 'Viral_Seq_Set',
    'annotation': 'Annotation',
    'clustered_seq_set': 'Clustered_Seq_set',
    '16s_prep': '16S_Prep',
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

edges = {
    'part_of': 'PART_OF',
    'participates_in': 'PARTICIPATES_IN',
    'associated_with': 'ASSOCIATED_WITH',
    'by': 'BY',
    'collected_during': 'COLLECTED_DURING',
    'prepared_from': 'PREPARED_FROM',
    'sequenced_from': 'SEQUENCED_FROM',
    'derived_from': 'DERIVED_FROM',
    'computed_from': 'COMPUTED_FROM'
}