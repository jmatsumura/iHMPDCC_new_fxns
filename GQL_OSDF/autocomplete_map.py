# This module contains all the information needed for populating the 
# auto-complete field. Namely, the description, the doc_type, the
# field, the full name, and the type of data.

gql_map = {}

# Project props
gql_map['project_name'] = {"description": "The project name", "doc_type": "cases", "field": "project name", "full": "cases.Project_name", "type": "string"}

# Study props
gql_map['study_subtype'] = {"description": "The subtype of the study", "doc_type": "cases", "field": "study subtype", "full": "cases.Study_subtype", "type": "string"}
gql_map['study_center'] = {"description": "The center/institute that conducted the study", "doc_type": "cases", "field": "study center", "full": "cases.Study_center", "type": "string"}
gql_map['study_name'] = {"description": "The name of the study", "doc_type": "cases", "field": "study name", "full": "cases.Study_name", "type": "string"}

# Subject props
gql_map['subject_gender'] = {"description": "The gender of the subject", "doc_type": "cases", "field": "subject gender", "full": "cases.Subject_gender", "type": "string"}
gql_map['subject_race'] = {"description": "The race of the subject", "doc_type": "cases", "field": "subject race", "full": "cases.Subject_race", "type": "string"}

# Visit props
gql_map['visit_number'] = {"description": "The number of the visit", "doc_type": "cases", "field": "visit number", "full": "cases.Visit_number", "type": "integer"}
gql_map['visit_interval'] = {"description": "The interval of the visits", "doc_type": "cases", "field": "visit interval", "full": "cases.Visit_interval", "type": "integer"}
gql_map['visit_date'] = {"description": "The date the visit occurred", "doc_type": "cases", "field": "visit date", "full": "cases.Visit_date", "type": "string"}

# Sample props. Note that this contains data within mixs nested JSON of OSDF.
gql_map['sample_biome'] = {"description": "The sample biome ENVO code", "doc_type": "cases", "field": "sample biome ENVO", "full": "cases.Sample_biome", "type": "string"}
gql_map['sample_body_product'] = {"description": "Material produced by the body site", "doc_type": "cases", "field": "body site product", "full": "cases.Sample_body_product", "type": "string"}
gql_map['sample_collection_date'] = {"description": "Date the sample was collected", "doc_type": "cases", "field": "sample collection date", "full": "cases.Sample_collection_date", "type": "string"}
gql_map['sample_env_package'] = {"description": "The environment the sample is associated with", "doc_type": "cases", "field": "sample environmental package", "full": "cases.Sample_env_package", "type": "string"}
gql_map['sample_feature'] = {"description": "The sample feature ENVO code", "doc_type": "cases", "field": "sample feature ENVO", "full": "cases.Sample_feature", "type": "string"}
gql_map['sample_fma_body_site'] = {"description": "The FMA body site the sample was derived from", "doc_type": "cases", "field": "sample FMA body site", "full": "sample.Sample_fma_body_site", "type": "string"}
gql_map['sample_geo_loc_name'] = {"description": "The geographical location the sample was derived from", "doc_type": "cases", "field": "sample geographical location", "full": "cases.Sample_geo_loc_name", "type": "string"}
gql_map['sample_lat_lon'] = {"description": "latitude and longitudinal coordinates where sample was extracted", "doc_type": "cases", "field": "lat/lon coordinates of sample", "full": "cases.Sample_lat_lon", "type": "string"}
gql_map['sample_material'] = {"description": "The sample material ENVO code", "doc_type": "cases", "field": "sample material ENVO", "full": "cases.Sample_material", "type": "string"}
gql_map['sample_project_name'] = {"description": "The project name associated with the sample", "doc_type": "cases", "field": "project name associated with sample", "full": "cases.Sample_project_name", "type": "string"}
gql_map['sample_rel_to_oxygen'] = {"description": "The relationship to oxygen", "doc_type": "cases", "field": "relationship to oxygen", "full": "cases.Sample_rel_to_oxygen", "type": "string"}
gql_map['sample_samp_collect_device'] = {"description": "The instrument used for collection of the sample", "doc_type": "cases", "field": "sample collection device", "full": "cases.samp_collect_device", "type": "string"}
gql_map['sample_samp_mat_process'] = {"description": "Details of the processing of the sample material", "doc_type": "cases", "field": "sample material processing", "full": "cases.Sample_mat_process", "type": "string"}
gql_map['sample_size'] = {"description": "The size of the sample", "doc_type": "cases", "field": "sample size", "full": "cases.Sample_samp_size", "type": "string"}
gql_map['sample_subtype'] = {"description": "The subtype of the sample", "doc_type": "cases", "field": "sample subtype", "full": "cases.Sample_subtype", "type": "string"}
gql_map['sample_supersite'] = {"description": "The supersite that encompasses the FMA body site", "doc_type": "cases", "field": "sample supersite", "full": "cases.Sample_supersite", "type": "string"}

# File props (includes everything below Sample node in OSDF schema)
gql_map['file_format'] = {"description": "The format of the file", "doc_type": "cases", "field": "file format", "full": "cases.File_format", "type": "string"}
gql_map['file_node_type'] = {"description": "The node type of the file", "doc_type": "cases", "field": "file node type", "full": "file.File_node_type", "type": "string"}
gql_map['file_annotation_pipeline'] = {"description": "The node type of the file", "doc_type": "cases", "field": "file node type", "full": "cases.File_node_type", "type": "string"}
gql_map['file_matrix_type'] = {"description": "The node type of the file", "doc_type": "cases", "field": "file node type", "full": "cases.File_node_type", "type": "string"}
# MIMARKS
gql_map['mimarks_adapters'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_biome'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_collection_date'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_env_package'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_experimental_factor'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_feature'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_findex'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_geo_loc_name'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_investigation_type'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_lat_lon'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_lib_const_meth'] = {"description": "MIMARKS - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_lib_reads_seqd'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_lib_size'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_lib_vector'] = {"description": "MIMARKS - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_material'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_nucl_acid_ext'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_pcr_cond'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_pcr_primers'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_project_name'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_rel_to_oxygen'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_rindex'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_samp_collect_device'] = {"description": "MIMARKS - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_samp_mat_process'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_samp_size'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_seq_meth'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_submitted_to_insdc'] = {"description": "MIMARKS - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_target_gene'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['mimarks_target_subfragment'] = {"description": "MIMARKS - ", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
# meta
gql_map['meta_comment'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_frag_size'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_lib_layout'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_lib_selection'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_ncbi_taxon_id'] = {"description": "Metadata - NCBI taxon ID", "doc_type": "cases", "field": "NCBI taxon ID", "full": "cases.File_ncbi_taxon_id", "type": "string"}
gql_map['meta_prep_id'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_sequencing_center'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_sequencing_contact'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_srs_id'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_storage_duration'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
gql_map['meta_subtype'] = {"description": "Metadata - comment", "doc_type": "cases", "field": "comment", "full": "cases.File_comment", "type": "string"}
