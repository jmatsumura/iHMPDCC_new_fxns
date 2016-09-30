# simple app to allow GraphiQL interaction with the schema and verify it is
# structured how it ought to be. 
from flask import Flask, jsonify, request
from flask_graphql import GraphQLView
from flask.views import MethodView
from sum_schema import sum_schema
from ac_schema import ac_schema
import graphene
import urllib2

app = Flask(__name__)
app.debug = True

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

sample_fma_body_site = {"description": "The FMA body site related to the sample", "doc_type": "cases", "field": "SampleFmabodysite", "full": "cases.SampleFmabodysite", "type": "string"}
project_name = {"description": "The Project Name", "doc_type": "cases", "field": "ProjectName", "full": "cases.ProjectName", "type": "string"}

@app.route('/gql/_mapping', methods=['GET'])
def get_maps():
    add_cors_headers
    res = jsonify({"cases.SampleFmabodysite": sample_fma_body_site, "cases.ProjectName": project_name})
    return res

@app.route('/cases', methods=['GET','OPTIONS'])
def get_cases():
    
    filters = request.args.get('filters')
    from_num = request.args.get('from')
    size = request.args.get('size')
    sort = request.args.get('sort')

    if(request.args.get('expand')):
        url = "http://localhost:5000/ac_schema?query=%7Bpagination%7Bcount%2Csort%2Cfrom%2Cpage%2Ctotal%2Cpages%2Csize%7D%2Chits%7Bproject%7Bproject_id%2Cdisease_type%2Cprimary_site%7D%7Daggregations%7BProjectName%7Bbuckets%7Bkey%2Cdoc_count%7D%7DSampleFmabodysite%7Bbuckets%7Bkey%2Cdoc_count%7D%7D%7D%7D"
        response = urllib2.urlopen(url)
        r = response.read()
        return ('%s, "warnings": {}}' % r[:-1])

    # Processing autocomplete here as well as finding counts for the set category
    if(request.args.get('facets')):

        beg = "http://localhost:5000/ac_schema?query=%7Bpagination%7Bcount%2Csort%2Cfrom%2Cpage%2Ctotal%2Cpages%2Csize%7D%2Chits%7Bproject%7Bproject_id%2Cdisease_type%2Cprimary_site%7D%7Daggregations%7B"
        mid = request.args.get('facets')
        end = "%7Bbuckets%7Bkey%2Cdoc_count%7D%7D%7D%7D"
        url = '%s%s%s' % (beg,mid,end)
        response = urllib2.urlopen(url)
        r = response.read()
        return ('%s, "warnings": {}}' % r[:-1])

    else:
        return jsonify({"filters": filters})

@app.route('/status', methods=['GET','OPTIONS'])
def get_status():
    return 'hi'

@app.route('/status/user', methods=['GET','OPTIONS','POST'])
def get_status_user():
    return 'hi'

@app.route('/files', methods=['GET','OPTIONS'])
def get_files():
    url = "http://localhost:5000/ac_schema?query=%7Bpagination%7Bcount%2Csort%2Cfrom%2Cpage%2Ctotal%2Cpages%2Csize%7D%2Chits%7Bproject%7Bproject_id%2Cdisease_type%2Cprimary_site%7D%7Daggregations%7BProjectName%7Bbuckets%7Bkey%2Cdoc_count%7D%7DSampleFmabodysite%7Bbuckets%7Bkey%2Cdoc_count%7D%7D%7D%7D"
    response = urllib2.urlopen(url)
    r = response.read()
    return ('%s, "warnings": {}}' % r[:-1])

@app.route('/projects', methods=['GET','POST'])
def get_project():
    return 'hi'

@app.route('/annotations', methods=['GET','OPTIONS'])
def get_annotation():
    return 'hi'

@app.route('/ui/search/summary', methods=['GET','OPTIONS','POST'])
def get_ui_search_summary():
    url = "http://localhost:5000/sum_schema?query=%7BSampleFmabodysite%7Bbuckets%7Bcase_count%2Cdoc_count%2Cfile_size%2Ckey%7D%7Dfs%7Bvalue%7D%7D"
    response = urllib2.urlopen(url)
    # another hack, remove "data" root from GQL results
    r1 = response.read()[8:]
    r2 = r1[:-1]
    return r2

app.add_url_rule(
    '/sum_schema',
    view_func=GraphQLView.as_view(
        'sum_graphql',
        schema=sum_schema,
        graphiql=True
    )
)

app.add_url_rule(
    '/ac_schema',
    view_func=GraphQLView.as_view(
        'ac_graphql',
        schema=ac_schema,
        graphiql=True
    )
)

if __name__ == '__main__':
    app.run(threaded=True)
