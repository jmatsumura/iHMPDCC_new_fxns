# This module contains all the information needed for populating the 
# auto-complete field. Namely, the description, the doc_type, the
# field, the full name, and the type of data.

gql_map = {}

# Project props
gql_map['project_name'] = {"description": "The Project Name", "doc_type": "cases", "field": "project name", "full": "cases.Project_name", "type": "string"}

# Study props

# Subject props
gql_map['subject_gender'] = {"description": "Gender of subject", "doc_type": "cases", "field": "subject gender", "full": "cases.SubjectGender", "type": "string"}

# Visit props

# Sample props
gql_map['sample_fma_body_site'] = {"description": "The FMA body site the sample was derived from", "doc_type": "cases", "field": "sample FMA body site", "full": "cases.Sample_fma_body_site", "type": "string"}
gql_map['sample_geo_loc_name'] = {"description": "The geographical location the sample was derived from", "doc_type": "cases", "field": "sample geographical location", "full": "cases.Sample_geo_loc_name", "type": "string"}
gql_map['sample_samp_collect_device'] = {"description": "The instrument used for collection of the sample", "doc_type": "cases", "field": "sample collection device", "full": "cases.samp_collect_device", "type": "string"}
gql_map['sample_env_package'] = {"description": "The environment the sample is associated with", "doc_type": "cases", "field": "sample environmental package", "full": "cases.Sample_env_package", "type": "string"}
gql_map['sample_supersite'] = {"description": "The supersite that encompasses the FMA body site", "doc_type": "cases", "field": "sample supersite", "full": "cases.Sample_supersite", "type": "string"}
gql_map['sample_feature'] = {"description": "The sample feature ENVO code", "doc_type": "cases", "field": "sample feature ENVO", "full": "cases.Sample_feature", "type": "string"}
gql_map['sample_material'] = {"description": "The sample material ENVO code", "doc_type": "cases", "field": "sample material ENVO", "full": "cases.Sample_material", "type": "string"}
gql_map['sample_biome'] = {"description": "The sample biome ENVO code", "doc_type": "cases", "field": "sample biome ENVO", "full": "cases.Sample_biome", "type": "string"}

# File props (includes everything below Sample node in OSDF schema)