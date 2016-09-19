from flask import Flask, jsonify
from flask import request
import copy
from py2neo import Graph

app = Flask(__name__)
neo4j_password = "osdf1"

graph = Graph(password=neo4j_password)
cypher = graph

#def auth_token_error():
#    res = jsonify({"error": 'token not found'})
#    res.status_code = 401
#    return res

#def auth_token():
#    token = request.args.get('token')
#    if token is not None:
#        # check if token exists
#        if token != 'A3144008131Z':
#            print "token not found!"
#            return auth_token_error()

#app.before_request(auth_token)

providers = {"serverType": "neo4j", "url": "http://localhost:5000", "version": "1"}

# filters that are available with this dataset
filter_contains = {"name": "contains", "operator": "contains", "valueType": "string",
                   "description": "string is contained within field value", "supportsNegate": "false"}

filter_equals = {"name": "equals", "operator": "equals", "valueType": "int", "description": "string equals field value",
                 "supportsNegate": "false"}

filter_range = {"name": "range", "operator": "range", "valueType": "int",
                "description": "field values are in this range", "supportsNegate": "true"}

# Study fields
#     "center",
#     "contact",
#     "description",
#     "name",
#     "tags"
#     "bp_id"
#     "srp_id"

field_study_name = {"field": "study_name", "type": "string", "description": "name of study", "label": "study_name",
                    "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_study_center = {"field": "study_center", "type": "string", "description": "center where study took place",
                      "label": "study_center",
                      "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_study_contact = {"field": "study_contact", "type": "string", "description": "contact info for study",
                       "label": "study_contact",
                       "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_study_bp_id = {"field": "study_bp_id", "type": "string",
                     "description": "NCBI Sequence Read Archive (SRA) project ID", "label": "study_bp_id",
                     "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_study_srp_id = {"field": "study_srp_id", "type": "string",
                      "description": "NCBI Sequence Read Archive (SRA) project ID", "label": "study_srp_id",
                      "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

# Subject fields
#     "rand_subject_id",
#     "gender",
#     "race"
field_subject_race = {"field": "race", "type": "string", "description": "host race", "label": "race",
                      "stats": {"rowCount": 0,
                                "distinctValues": ["african_american", "american_indian_or_alaska_native",
                                                   "asian", "caucasian", "hispanic_or_latino", "native_hawaiian",
                                                   "ethnic_other", "unknown"]}, "filter": [filter_contains]}

field_subject_gender = {"field": "gender", "type": "string", "description": "host gender", "label": "gender",
                        "stats": {"rowCount": 0, "distinctValues": ["male", "female", "unknown"]},
                        "filter": [filter_equals]}

field_subject_rand_subject_id = {"field": "rand_subject_id", "type": "string",
                                 "description": "rand_subject_id of subject", "label": "rand_subject_id",
                                 "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

# Visit fields
#     "interval",
#     "tags",
#     "visit_id",
#     "visit_number"
#     "date"
#     "clinic_id"

field_visit_date = {"field": "visit_date", "type": "string", "description": "date of visit", "label": "visit_date",
                    "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_visit_id = {"field": "visit_id", "type": "string", "description": "id of visit", "label": "visit_id",
                  "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_visit_number = {"field": "visit_number", "type": "int", "description": "visit number of sample",
                      "label": "visit_number",
                      "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_range]}

field_visit_interval = {"field": "visit_interval", "type": "int", "description": "interval between visits",
                        "label": "visit_interval", "stats": {"rowCount": 0, "distinctValues": " "},
                        "filter": [filter_range]}

field_visit_clinic_id = {"field": "clinic_id", "type": "string", "description": "clinic id for the visit",
                         "label": "clinic_id", "stats": {"rowCount": 0, "distinctValues": " "},
                         "filter": [filter_contains]}

# Sample fields
#    body_site
#    fma_body_site
#    mixs
#    name
#    supersite
#    mixs

body_sites = ["anterior_nares", "attached_keratinized_gingiva",
              "buccal_mucosa", "hard_palate", "left_antecubital_fossa",
              "left_retroauricular_crease", "mid_vagina", "palatine_tonsils",
              "posterior_fornix", "right_antecubital_fossa",
              "right_retroauricular_crease", "saliva", "stool",
              "subgingival_plaque", "supragingival_plaque", "throat",
              "tongue_dorsum", "vaginal_introitus", "ileal_pouch", "cervix",
              "perianal_region", "wall_of_vagina", "oral_cavity", "ileum",
              "blood", "bone", "cerebrospinal_fluid", "ear", "heart", "liver",
              "lymph_node", "spinal_cord", "elbow", "knee", "abdomen", "thigh",
              "leg", "forearm", "volar_forearm", "scalp", "shoulder", "nare",
              "shin", "back", "foot", "hand", "popliteal_fossa",
              "antecubital_fossa", "appendix", "ascending_colon", "colon",
              "conjunctiva", "dental_plaque", "descending_colon", "duodenum",
              "endometrium", "foregut", "gall_bladder", "gastric_antrum",
              "gingival_crevices", "gum_margin_of_molar_tooth_on_buccal_side",
              "gut", "ileal-anal_pouch", "intestinal_tract", "left_arm",
              "lung_aspirate", "lymph_nodes", "mouth", "nasal", "nasopharynx",
              "periodontal", "pharyngeal_mucosa", "rectal", "respiratory_tract",
              "right_arm", "sigmoid_colon", "stomach", "subgingival",
              "synovial_fluid", "teeth", "terminal_ileum", "transverse_colon",
              "unknown", "upper_respiratory_tract", "urethra", "urinary_tract",
              "vaginal", "wound"]

supersites = ["airways", "blood", "bone", "brain", "ear", "eye",
              "gastrointestinal_tract", "heart", "lymph_node",
              "liver", "lymph_nodes", "oral", "other", "skin",
              "spinal_cord", "unknown", "urogenital_tract", "wound"]

fma_body_sites = ["unknown"]

field_sample_body_site = {"field": "body_site", "type": "string", "description": "host body site of sample",
                          "label": "body_site",
                          "stats": {"rowCount": 0, "distinctValues": body_sites}, "filter": [filter_contains]}

field_sample_fma_body_site = {"field": "fma_body_site", "type": "string",
                              "description": "FMA ontology host body site of sample", "label": "fma_body_site",
                              "stats": {"rowCount": 0, "distinctValues": fma_body_sites}, "filter": [filter_contains]}

field_sample_supersite = {"field": "supersite", "type": "string", "description": "host HMP super site of sample",
                          "label": "supersite",
                          "stats": {"rowCount": 0, "distinctValues": supersites}, "filter": [filter_contains]}

field_sample_name = {"field": "sample_name", "type": "string", "description": "sample name", "label": "sample_name",
                     "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

# 16sDNAPrep fields
# "comment",
#  frag_size
# "lib_layout",
# "lib_selection",
# "mimarks",
# "ncbi_taxon_id",
# "prep_id",
# "sequencing_center",
# "sequencing_contact",
# "storage_duration",
#  srs_id

field_16SDNAPrep_comment = {"field": "16SDNAPrep_comment", "type": "string", "description": "Free-text comment",
                            "label": "16SDNAPrep_comment",
                            "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_16SDNAPrep_frag_size = {"field": "16SDNAPrep_frag_size", "type": "integer",
                              "description": "Target library fragment size after shearing",
                              "label": "16SDNAPrep_frag_size",
                              "stats": {"rowCount": 0, "distinctValues": " "}, "filter": [filter_contains]}

field_16SDNAPrep_lib_layout = {"field": "16SDNAPrep_lib_layout", "type": "string", "description": "Specification of the\
                    layout: fragment/paired, and if paired, then nominal insert size and standard deviationt",
                               "label": "16SDNAPrep_lib_layout", "stats": {"rowCount": 0, "distinctValues": " "},
                               "filter": [filter_contains]}

field_16SDNAPrep_lib_selection = {"field": "16SDNAPrep_lib_selection", "type": "string", "description": "A controlled \
                        vocabulary of terms describing selection or reduction method used in library construction. \
                        Terms used by TCGA include (random, hybrid selection)",
                                  "label": "16SDNAPrep_lib_selection", "stats": {"rowCount": 0, "distinctValues": " "},
                                  "filter": [filter_contains]}

field_16SDNAPrep_mimarks = {"field": "16SDNAPrep_mimarks", "type": "string",
                            "description": "Genomic Standards Consortium MIMARKS fields",
                            "label": "16SDNAPrep_mimarks", "stats": {"rowCount": 0, "distinctValues": " "},
                            "filter": [filter_contains]}

field_16SDNAPrep_ncbi_taxon_id = {"field": "16SDNAPrep_ncbi_taxon_id", "type": "string", "description": "NCBI taxon id",
                                  "label": "16SDNAPrep_ncbi_taxon_id", "stats": {"rowCount": 0, "distinctValues": " "},
                                  "filter": [filter_contains]}

field_16SDNAPrep_prep_id = {"field": "16SDNAPrep_prep_id", "type": "string", "description": "Nucleic Acid Prep ID",
                            "label": "16SDNAPrep_prep_id", "stats": {"rowCount": 0, "distinctValues": " "},
                            "filter": [filter_contains]}

field_16SDNAPrep_sequencing_center = {"field": "16SDNAPrep_sequencing_center", "type": "string",
                                      "description": "The center responsible for generating the 16S DNA Prep",
                                      "label": "16SDNAPrep_sequencing_center",
                                      "stats": {"rowCount": 0, "distinctValues": " "},
                                      "filter": [filter_contains]}

field_16SDNAPrep_sequencing_contact = {"field": "16SDNAPrep_sequencing_contact", "type": "string",
                                       "description": "Name and email of the primary contact at the sequencing center",
                                       "label": "16SDNAPrep_sequencing_contact",
                                       "stats": {"rowCount": 0, "distinctValues": " "},
                                       "filter": [filter_contains]}

field_16SDNAPrep_srs_id = {"field": "16SDNAPrep_srs_id", "type": "string",
                           "description": "NCBI Sequence Read Archive sample ID of the form SRS012345",
                           "label": "16SDNAPrep_srs_id", "stats": {"rowCount": 0, "distinctValues": " "},
                           "filter": [filter_contains]}

field_16SDNAPrep_storage_duration = {"field": "16SDNAPrep_storage_duration", "type": "integer",
                                     "description": "Duration for which sample was stored in days",
                                     "label": "16SDNAPrep_storage_duration",
                                     "stats": {"rowCount": 0, "distinctValues": " "},
                                     "filter": [filter_contains]}
annotations = []

# Study fields
annotations.append(field_study_center)
annotations.append(field_study_name)
annotations.append(field_study_contact)
annotations.append(field_study_bp_id)
annotations.append(field_study_srp_id)

# Subject fields
annotations.append(field_subject_gender)
annotations.append(field_subject_race)
annotations.append(field_subject_rand_subject_id)

# Visit fields
annotations.append(field_visit_id)
annotations.append(field_visit_number)
annotations.append(field_visit_interval)
annotations.append(field_visit_date)
annotations.append(field_visit_clinic_id)

# Sample fields
annotations.append(field_sample_body_site)
annotations.append(field_sample_fma_body_site)
annotations.append(field_sample_supersite)
annotations.append(field_sample_name)

# 16sDNAPrep fields
# annotations.append(field_16SDNAPrep_comment)
# annotations.append(field_16SDNAPrep_frag_size)
# annotations.append(field_16SDNAPrep_lib_layout)
# annotations.append(field_16SDNAPrep_lib_selection)
# annotations.append(field_16SDNAPrep_mimarks)
# annotations.append(field_16SDNAPrep_ncbi_taxon_id)
# annotations.append(field_16SDNAPrep_prep_id)
# annotations.append(field_16SDNAPrep_sequencing_center)
# annotations.append(field_16SDNAPrep_sequencing_contact)
# annotations.append(field_16SDNAPrep_srs_id)
# annotations.append(field_16SDNAPrep_storage_duration)

pre_processed_query1 = {"queryName": "All Males", "filters": [
    {"filterField": "gender", "filterName": "equals", "filterValue": "male", "negate": "false"}]}
pre_processed_query2 = {"queryName": "All Males Foot", "filters": [
    {"filterField": "gender", "filterName": "equals", "filterValue": "male", "negate": "false"},
    {"filterField": "body_site", "filterName": "contains", "filterValue": "foot", "negate": "false"}]}
pre_processed_query3 = {"queryName": "All Saliva Visit Number 1 ", "filters": [
    {"filterField": "body_site", "filterName": "contains", "filterValue": "saliva", "negate": "false"},
    {"filterField": "visit_number", "filterName": "contains", "filterValue": "1", "negate": "false"}]}

# Function to handle access control allow headers
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = 'http://localhost:3000'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response


app.after_request(add_cors_headers)


# Function for displaying data providers available with data sources
@app.route('/dataProviders', methods=['GET'])
def get_providers():
    res = jsonify({"dataProviders": providers})
    return res


# Function for displaying all data sources served by this provider
@app.route('/dataSources', methods=['GET'])
def get_sources():
    # session = driver.session()
    sources = []
    result = cypher.run("MATCH (n:Project) RETURN n.id AS id, n.name AS name, n.description AS description")
    print(result)
    print(result.keys())
    for record in result:
        sources_dict = {}
        sources_dict['name'] = record['name']
        sources_dict['description'] = record['description']
        sources_dict['version'] = '1'
        sources.append(copy.deepcopy(sources_dict))
        #print(sources_dict)

    # session.close()

    res = jsonify({"dataSources": sources})

    return res

@app.route('/status', methods=['GET', ])
def get_status():
    add_cors_headers
    return 'hi', 200

@app.route('/status/user', methods=['GET', 'POST'])
def get_user_status():
    add_cors_headers
    return 'hi', 200

@app.route('/projects', methods=['GET', 'OPTIONS'])
def get_projects():
    add_cors_headers
    return 'hi', 200

@app.route('/ui/search/summary', methods=['GET', 'OPTIONS'])
def get_ui_search_summary():
    add_cors_headers
    return 'hi', 200

@app.route('/files', methods=['GET', 'OPTIONS'])
def get_files():
    add_cors_headers
    return 'hi', 200

@app.route('/cases', methods=['GET', 'OPTIONS'])
def get_cases():
    add_cors_headers
    return 'hi', 200

@app.route('/gql/_mapping', methods=['GET', 'OPTIONS'])
def gql_mapping():
    add_cors_headers
    return 'hi', 200

# Function for performing SQL query to retrieve database attributes
@app.route('/annotations/<dsName>', methods=['GET'])
def get_annotations(dsName):
    dataSource = dsName
    formatRes = request.args.get('format')
    res = jsonify({"dataSource": dataSource, "dataAnnotations": annotations})
    return res


# Function for performing SQL query to retrieve measurements with filters and pagination
@app.route('/measurements/<dsName>', methods=['POST', 'OPTIONS'])
def post_measurements(dsName):
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
        return res

    request.data = request.get_json()
    #print(request.data)
    reqId = request.args.get('requestId')

    keys = ["study_center", "study_name", "race", "gender", "visit_id", "visit_number", "visit_interval", "body_site",
            "fma_body_site", "supersite", "name", "unique_id", "visit_unique_id"]

    measurements = []
    dictionary = {}
    for i in range(0, len(keys)):
        dictionary[keys[i]] = " "

    projectReqStr = ''
    studyReqStr = ''
    subjectReqStr = ''
    visitReqStr = ''
    sampleReqStr = ''
    dnaPrep16sReqStr = ''

    # session = driver.session()

    if len(request.data['filter']) == 0:
        result = cypher.run(
            "MATCH (pr:Project {name: '" + dsName + "'}) MATCH (st:Study {}) MATCH (sb:Subject {}) MATCH (v:Visit {}) MATCH (sa:Sample {}) MATCH (sq:DNAPrep16s {})\
            OPTIONAL MATCH (sq)-[*]->(sa) OPTIONAL MATCH (sa)-[*]->(v) OPTIONAL MATCH (v)-[*]->(sb) \
            RETURN distinct st.center, st.name, sb.race, sb.gender, v.visit_id, \
            v.visit_number, v.visit_interval, sa.body_site, sa.fma_body_site, sa.supersite, sa.name, sa._id LIMIT "+ str(request.data['pageSize']))

    # Will need to differentiate between contains, equals, range with Cypher query


    if len(request.data['filter']) > 0:
        #print(request.data['filter'])
        for i in range(0, len(request.data['filter'])):
            if request.data['filter'][i]['filterField'] == 'project_name':
                projectReqField = request.data['filter'][i]['filterField']
                projectReqValue = request.data['filter'][i]['filterValue']
                projectReqStr = projectReqField + ": '" + projectReqValue + "'"
                print("project request string: " + projectReqStr)
            elif request.data['filter'][i]['filterField'] == 'study_center' or request.data['filter'][i][
                'filterField'] == 'study_name' or request.data['filter'][i]['filterField'] == 'study_bp_id' or \
                            request.data['filter'][i]['filterField'] == 'study_srp_id':
                studyReqField = request.data['filter'][i]['filterField']
                studyReqValue = request.data['filter'][i]['filterValue']
                studyReqStr = studyReqField + ": '" + studyReqValue + "'"
                print("study request string: " + studyReqStr)
            elif request.data['filter'][i]['filterField'] == 'gender' or request.data['filter'][i][
                'filterField'] == 'rand_subject_id' or request.data['filter'][i]['filterField'] == 'race':
                subjectReqField = request.data['filter'][i]['filterField']
                subjectReqValue = request.data['filter'][i]['filterValue']
                subjectReqStr = subjectReqField + ": '" + subjectReqValue + "'"
                print("subject request string: " + subjectReqStr)
            elif request.data['filter'][i]['filterField'] == 'visit_id' or request.data['filter'][i][
                'filterField'] == 'visit_number' or request.data['filter'][i]['filterField'] == 'visit_interval' \
                    or request.data['filter'][i]['filterField'] == 'clinic_id' or request.data['filter'][i][
                'filterField'] == 'visit_date':
                visitReqField = request.data['filter'][i]['filterField']
                visitReqValue = request.data['filter'][i]['filterValue']
                visitReqStr = visitReqField + ": '" + visitReqValue + "'"
                print("visit request string: " + visitReqStr)
            elif request.data['filter'][i]['filterField'] == 'body_site' or request.data['filter'][i][
                'filterField'] == 'fma_body_site' or request.data['filter'][i]['filterField'] == 'supersite' or \
                            request.data['filter'][i]['filterField'] == 'sample_name':
                sampleReqField = request.data['filter'][i]['filterField']
                sampleReqValue = request.data['filter'][i]['filterValue']
                sampleReqStr = sampleReqField + ": '" + sampleReqValue + "'"
                print("sample request string: " + sampleReqStr)
            elif request.data['filter'][i]['filterField'] == '16SDNAPrep_frag_size' or request.data['filter'][i][
                'filterField'] == '16SDNAPrep_lib_selection':
                dnaPrep16sReqField = request.data['filter'][i]['filterField']
                dnaPrep16sReqValue = request.data['filter'][i]['filterValue']
                dnaPrep16sReqStr = dnaPrep16sReqField + ": '" + dnaPrep16sReqValue + "'"
                print("dnaPrep16s request string: " + dnaPrep16sReqStr)

        queryStr = "MATCH (pr:Project {name: '" + dsName + "'}) MATCH (st:Study {" + studyReqStr + "}) MATCH (sb:Subject {" + subjectReqStr + "}) \
                    MATCH (v:Visit {" + visitReqStr + "}) MATCH (sa:Sample {" + sampleReqStr + "}) \
                    MATCH (sa)-[*]->(v) OPTIONAL MATCH (v)-[*]->(sb) OPTIONAL MATCH (sb)-[*]->(st) \
                    OPTIONAL MATCH (st)-[*]->(p) RETURN distinct st.center, st.name, sb.race, sb.gender, v.visit_id, \
                    v.visit_number, v.visit_interval, sa.body_site, sa.fma_body_site, sa.supersite, sa.name, sa._id, v._id SKIP " \
                    + str(request.data['pageOffset']) + " LIMIT " + str(request.data['pageSize'])

        print(queryStr)
        result = cypher.run(queryStr)

    list_result = list(result)
    print("About to print results")
    for j in range(0, len(list_result)):
        measurements.append(copy.deepcopy(dictionary))
        for k in range(0, len(list_result[j])):
            measurements[j][keys[k]] = list_result[j][k]
    print("Printed results")

    # session.close()

    pageSize = str(10)
    res = jsonify({"dataMeasurements": measurements, "totalCount": len(measurements),
                   "pageOffset": str(request.data['pageOffset']),
                   "requestId": reqId})
    return res


# Function for performing SQL query to retrieve database attributes
@app.route('/queries/<dsName>', methods=['GET'])
def get_queries(dsName):
    dataSource = dsName
    res = jsonify(
        {"dataSource": dataSource, "queries": [pre_processed_query1, pre_processed_query2, pre_processed_query3]})
    return res


if __name__ == '__main__':
    app.run(debug=True)