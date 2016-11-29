# This module contains all the information needed for populating the 
# auto-complete field. Namely, the description, the doc_type, the
# field, the full name, and the type of data.

gql_map = {}

# Project props
gql_map['project_name'] = {"description": "The name of the project within which the sequencing was organized", "doc_type": "cases", "field": "project name", "full": "cases.Project_name", "type": "string"}
gql_map['project_subtype'] = {"description": "The subtype of the project: healthy_human, ihmp, or demo", "doc_type": "cases", "field": "project subtype", "full": "cases.Project_subtype", "type": "string"}

# Study props
gql_map['study_center'] = {"description": "The study's primary contact at the sequencing center", "doc_type": "cases", "field": "study center", "full": "cases.Study_center", "type": "string"}
gql_map['study_description'] = {"description": "A longer description of the study", "doc_type": "cases", "field": "study description", "full": "cases.Study_description", "type": "string"}
gql_map['study_name'] = {"description": "The name of the study", "doc_type": "cases", "field": "study name", "full": "cases.Study_name", "type": "string"}
gql_map['study_srp_id'] = {"description": "NCBI Sequence Read Archive (SRA) project ID", "doc_type": "cases", "field": "study SRP ID", "full": "cases.Study_srp_id", "type": "string"}
gql_map['study_subtype'] = {"description": "The subtype of the study", "doc_type": "cases", "field": "study subtype", "full": "cases.Study_subtype", "type": "string"}

# Subject props
gql_map['subject_gender'] = {"description": "The subject's sex", "doc_type": "cases", "field": "subject gender", "full": "cases.Subject_gender", "type": "string"}
gql_map['subject_race'] = {"description": "The subject's race/ethnicity", "doc_type": "cases", "field": "subject race", "full": "cases.Subject_race", "type": "string"}

# Visit props
gql_map['visit_date'] = {"description": "Date when the visit occurred", "doc_type": "cases", "field": "visit date", "full": "cases.Visit_date", "type": "string"}
gql_map['visit_interval'] = {"description": "The amount of time since the last visit (in days)", "doc_type": "cases", "field": "visit interval", "full": "cases.Visit_interval", "type": "integer"}
gql_map['visit_number'] = {"description": "A sequential number that is assigned as visits occur for that subject", "doc_type": "cases", "field": "visit number", "full": "cases.Visit_number", "type": "integer"}

# Sample props. Note that this contains data within mixs nested JSON of OSDF.
gql_map['sample_biome'] = {"description": "Biomes are defined based on factors such as plant structures, leaf types, plant spacing, and other factors like climate", "doc_type": "cases", "field": "sample biome ENVO", "full": "sample.Sample_biome", "type": "string"}
gql_map['sample_body_product'] = {"description": "Substance produced by the body, e.g. stool, mucus, where the sample was obtained from", "doc_type": "cases", "field": "sample body product", "full": "sample.Sample_body_product", "type": "string"}
gql_map['sample_collection_date'] = {"description": "The time of sampling, either as an instance (single point in time) or interval", "doc_type": "cases", "field": "sample collection date", "full": "sample.Sample_collection_date", "type": "string"}
gql_map['sample_env_package'] = {"description": "Controlled vocabulary of MIGS/MIMS environmental packages", "doc_type": "cases", "field": "sample environmental package", "full": "sample.Sample_env_package", "type": "string"}
gql_map['sample_feature'] = {"description": "Environmental feature level includes geographic environmental features", "doc_type": "cases", "field": "sample feature ENVO", "full": "sample.Sample_feature", "type": "string"}
gql_map['sample_fma_body_site'] = {"description": "Body site from which the sample was obtained using the FMA ontology", "doc_type": "cases", "field": "sample FMA body site", "full": "cases.Sample_fma_body_site", "type": "string"}
gql_map['sample_geo_loc_name'] = {"description": "The geographical origin of the sample as defined by the country or sea name followed by specific region name", "doc_type": "cases", "field": "sample geographical location", "full": "sample.Sample_geo_loc_name", "type": "string"}
gql_map['sample_lat_lon'] = {"description": "Latitude/longitude in WGS 84 coordinates", "doc_type": "cases", "field": "lat/lon coordinates of sample", "full": "sample.Sample_lat_lon", "type": "string"}
gql_map['sample_material'] = {"description": "Matter that was displaced by the sample, before the sampling event", "doc_type": "cases", "field": "sample material ENVO", "full": "sample.Sample_material", "type": "string"}
gql_map['sample_project_name'] = {"description": "Name of the project within which the sequencing was organized", "doc_type": "cases", "field": "project name associated with sample", "full": "sample.Sample_project_name", "type": "string"}
gql_map['sample_rel_to_oxygen'] = {"description": "Whether the organism is an aerobe or anaerobe", "doc_type": "cases", "field": "relationship to oxygen", "full": "sample.Sample_rel_to_oxygen", "type": "string"}
gql_map['sample_samp_collect_device'] = {"description": "The method or device employed for collecting the sample", "doc_type": "cases", "field": "sample collection device", "full": "sample.samp_collect_device", "type": "string"}
gql_map['sample_samp_mat_process'] = {"description": "Any processing applied to the sample during or after retrieving the sample from environment", "doc_type": "cases", "field": "sample material processing", "full": "sample.Sample_mat_process", "type": "string"}
gql_map['sample_size'] = {"description": "Amount or size of sample (volume, mass or area) that was collected", "doc_type": "cases", "field": "sample size", "full": "sample.Sample_samp_size", "type": "string"}
gql_map['sample_subtype'] = {"description": "The subtype of the sample", "doc_type": "cases", "field": "sample subtype", "full": "sample.Sample_subtype", "type": "string"}
gql_map['sample_supersite'] = {"description": "Body supersite from which the sample was obtained", "doc_type": "cases", "field": "sample supersite", "full": "sample.Sample_supersite", "type": "string"}

# File props (includes everything below Sample node in OSDF schema)
gql_map['file_format'] = {"description": "The format of the file", "doc_type": "cases", "field": "file format", "full": "cases.File_format", "type": "string"}
gql_map['file_node_type'] = {"description": "The node type of the file", "doc_type": "cases", "field": "file node type", "full": "file.File_node_type", "type": "string"}
gql_map['file_annotation_pipeline'] = {"description": "The annotation pipeline used to generate the file", "doc_type": "cases", "field": "annotation pipeline which generated file", "full": "cases.File_annotation_pipeline", "type": "string"}
gql_map['file_matrix_type'] = {"description": "The type of matrix format present in the file", "doc_type": "cases", "field": "matrix format", "full": "cases.File_matrix_type", "type": "string"}
# MIMARKS
gql_map['mimarks_adapters'] = {"description": "Adapters provide priming sequences for both amplification and sequencing of the sample-library fragments", "doc_type": "cases", "field": "MIMARKS - adapters", "full": "file.File_adapters", "type": "string"}
gql_map['mimarks_biome'] = {"description": "Biomes are defined based on factors such as plant structures, leaf types, plant spacing, and other factors like climate", "doc_type": "cases", "field": "MIMARKS - biome", "full": "file.File_biome", "type": "string"}
gql_map['mimarks_collection_date'] = {"description": "The time of sampling, either as an instance (single point in time) or interval", "doc_type": "cases", "field": "MIMARKS - collection date", "full": "file.File_collection_date", "type": "string"}
gql_map['mimarks_env_package'] = {"description": "Controlled vocabulary of MIMARKS environmental packages", "doc_type": "cases", "field": "MIMARKS - environmental package", "full": "file.File_env_package", "type": "string"}
gql_map['mimarks_experimental_factor'] = {"description": "The variable aspects of an experiment design which can be used to describe an experiment, or set of experiments, in an increasingly detailed manner", "doc_type": "cases", "field": "MIMARKS - experimental factor", "full": "file.File_experimental_factor", "type": "string"}
gql_map['mimarks_feature'] = {"description": "Environmental feature level includes geographic environmental features", "doc_type": "cases", "field": "MIMARKS - feature", "full": "file.File_feature", "type": "string"}
gql_map['mimarks_findex'] = {"description": "Forward strand molecular barcode, called Multiplex Identifier (MID), that is used to specifically tag unique samples in a sequencing run", "doc_type": "cases", "field": "MIMARKS - findex", "full": "file.File_findex", "type": "string"}
gql_map['mimarks_geo_loc_name'] = {"description": "The geographical origin of the sample as defined by the country or sea name followed by specific region name", "doc_type": "cases", "field": "MIMARKS - geography location name", "full": "file.File_geo_loc_name", "type": "string"}
gql_map['mimarks_investigation_type'] = {"description": "This field is either MIMARKS survey or MIMARKS specimen", "doc_type": "cases", "field": "MIMARKS - investigation type", "full": "file.File_investigation_type", "type": "string"}
gql_map['mimarks_isol_growth_condt'] = {"description": "Publication reference in the form of pubmed ID (pmid), digital object identifier (doi) or url for isolation and growth condition specifications of the organism/material", "doc_type": "cases", "field": "MIMARKS - isolation and growth conditions", "full": "file.File_isol_growth_condt", "type": "string"}
gql_map['mimarks_lat_lon'] = {"description": "Latitude/longitude in WGS 84 coordinates", "doc_type": "cases", "field": "MIMARKS - latitude / longitude", "full": "file.File_lat_lon", "type": "string"}
gql_map['mimarks_lib_const_meth'] = {"description": "Library construction method used for clone libraries", "doc_type": "cases", "field": "MIMARKS - library construction method", "full": "file.File_lib_const_meth", "type": "string"}
gql_map['mimarks_lib_reads_seqd'] = {"description": "Total number of clones sequenced from the library", "doc_type": "cases", "field": "MIMARKS - library reads sequence", "full": "file.File_lib_reads_seqd", "type": "string"}
gql_map['mimarks_lib_size'] = {"description": "Total number of clones in the library prepared for the project", "doc_type": "cases", "field": "MIMARKS - clone library size", "full": "file.File_lib_size", "type": "string"}
gql_map['mimarks_lib_vector'] = {"description": "Cloning vector type(s) used in construction of libraries", "doc_type": "cases", "field": "MIMARKS - cloning vector types", "full": "file.File_lib_vector", "type": "string"}
gql_map['mimarks_material'] = {"description": "Matter that was displaced by the sample, before the sampling event", "doc_type": "cases", "field": "MIMARKS - material", "full": "file.File_material", "type": "string"}
gql_map['mimarks_nucl_acid_amp'] = {"description": "Link to a literature reference, electronic resource or a standard operating procedure (SOP)", "doc_type": "cases", "field": "MIMARKS - nucleic acid amplification", "full": "file.File_nucl_acid_amp", "type": "string"}
gql_map['mimarks_nucl_acid_ext'] = {"description": "Link to a literature reference, electronic resource or a standard operating procedure (SOP)", "doc_type": "cases", "field": "MIMARKS - nucleic acid extraction", "full": "file.File_nucl_acid_ext", "type": "string"}
gql_map['mimarks_pcr_cond'] = {"description": "PCR condition used to amplify the sequence of the targeted gene, locus or sub-fragment", "doc_type": "cases", "field": "MIMARKS - pcr conditions", "full": "file.File_pcr_cond", "type": "string"}
gql_map['mimarks_pcr_primers'] = {"description": "PCR primers that were used to amplify the sequence of the targeted gene, locus or subfragment", "doc_type": "cases", "field": "MIMARKS - PCR primers", "full": "file.File_pcr_primers", "type": "string"}
gql_map['mimarks_project_name'] = {"description": "Name of the project within which the sequencing was organized", "doc_type": "cases", "field": "MIMARKS - project name", "full": "file.File_project_name", "type": "string"}
gql_map['mimarks_rel_to_oxygen'] = {"description": "Whether the organism is an aerobe or anaerobe", "doc_type": "cases", "field": "MIMARKS - relationship to oxygen", "full": "file.File_rel_to_oxygen", "type": "string"}
gql_map['mimarks_rindex'] = {"description": "Reverse strand molecular barcode, called Multiplex Identifier (MID), that is used to specifically tag unique samples in a sequencing run", "doc_type": "cases", "field": "MIMARKS - R index", "full": "file.File_rindex", "type": "string"}
gql_map['mimarks_samp_collect_device'] = {"description": "The method or device employed for collecting the sample", "doc_type": "cases", "field": "MIMARKS - sample collection device", "full": "file.File_samp_collect_device", "type": "string"}
gql_map['mimarks_samp_mat_process'] = {"description": "Any processing applied to the sample during or after retrieving the sample from environment", "doc_type": "cases", "field": "MIMARKS - sample material processing", "full": "file.File_samp_mat_process", "type": "string"}
gql_map['mimarks_samp_size'] = {"description": "Amount or size of sample (volume, mass or area) that was collected", "doc_type": "cases", "field": "MIMARKS - sample size", "full": "file.File_samp_size", "type": "string"}
gql_map['mimarks_seq_meth'] = {"description": "Sequencing method used; e.g. Sanger, pyrosequencing, ABI-solid", "doc_type": "cases", "field": "MIMARKS - sequencing method", "full": "file.File_seq_meth", "type": "string"}
gql_map['mimarks_submitted_to_insdc'] = {"description": "Depending on the study (large scale, eg: next generation sequencing technology, or small scale) sequences have to submitted to SRA (Sequence Read Archive), DRA (DDBJ Read Archive) or via the classical WEBIN/Sequin systems to GenBank, ENA, and DDBJ", "doc_type": "cases", "field": "MIMARKS - submission center", "full": "file.File_submitted_to_insdc", "type": "string"}
gql_map['mimarks_target_gene'] = {"description": "Targeted gene or locus name for marker gene studies", "doc_type": "cases", "field": "MIMARKS - target gene", "full": "file.File_target_gene", "type": "string"}
gql_map['mimarks_target_subfragment'] = {"description": "Name of subfragment of a gene or locus", "doc_type": "cases", "field": "MIMARKS - target subfragment", "full": "file.File_target_subfragment", "type": "string"}
# meta
gql_map['meta_comment'] = {"description": "Free-text comment", "doc_type": "cases", "field": "comment", "full": "file.File_comment", "type": "string"}
gql_map['meta_frag_size'] = {"description": "Target library fragment size after shearing", "doc_type": "cases", "field": "Metadata - fragment size", "full": "file.File_frag_size", "type": "string"}
gql_map['meta_lib_layout'] = {"description": "Specification of the layout: fragment/paired, and if paired, then nominal insert size and standard deviation", "doc_type": "cases", "field": "Metadata - library layout specification", "full": "file.File_lib_layout", "type": "string"}
gql_map['meta_lib_selection'] = {"description": "A controlled vocabulary of terms describing selection or reduction method used in library construction", "doc_type": "cases", "field": "Metadata - library selection", "full": "file.File_lib_selection", "type": "string"}
gql_map['meta_ncbi_taxon_id'] = {"description": "NCBI taxon id", "doc_type": "cases", "field": "Metadata - NCBI taxon ID", "full": "file.File_ncbi_taxon_id", "type": "string"}
gql_map['meta_prep_id'] = {"description": "Nucleic Acid Prep ID", "doc_type": "cases", "field": "Metadata - prep ID", "full": "file.File_prep_id", "type": "string"}
gql_map['meta_sequencing_center'] = {"description": "The center responsible for generating the prep", "doc_type": "cases", "field": "Metadata - sequencing center", "full": "file.File_sequencing_center", "type": "string"}
gql_map['meta_sequencing_contact'] = {"description": "Name and email of the primary contact at the sequencing center", "doc_type": "cases", "field": "Metadata - sequencing contact", "full": "file.File_sequencing_contact", "type": "string"}
gql_map['meta_srs_id'] = {"description": "NCBI Sequence Read Archive sample ID of the form SRS012345", "doc_type": "cases", "field": "Metadata - SRS ID", "full": "file.File_srs_id", "type": "string"}
gql_map['meta_storage_duration'] = {"description": "Duration for which sample was stored in days", "doc_type": "cases", "field": "Metadata - storage duration", "full": "file.File_storage_duration", "type": "string"}
gql_map['meta_subtype'] = {"description": "The subtype of the DNA prep", "doc_type": "cases", "field": "Metadata - subtype", "full": "file.File_subtype", "type": "string"}
