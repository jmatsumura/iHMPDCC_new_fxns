# simple app to allow GraphiQL interaction with the schema and verify it is
# structured how it ought to be. 
from flask import Flask, jsonify, request
from flask_graphql import GraphQLView
from flask.views import MethodView
from schema import schema

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

sample_fma_body_site = {"description": "sfbs", "doc_type": "cases", "field": "fma_body_site", "full": "sample.fma_body_site", "type": "string"}
project_name = {"description": "pn", "doc_type": "cases", "field": "name", "full": "project.name", "type": "string"}

@app.route('/gql/_mapping', methods=['GET'])
def get_maps():
    add_cors_headers
    res = jsonify({"sample.fma_body_site": sample_fma_body_site, "project.name": project_name})
    return res

# Files
class FilesAPI(MethodView):

    def get(self):
        res = jsonify({"project.name": project_name, "sample.fma_body_site": sample_fma_body_site})
        return res

    def options(self):
        res = jsonify({"project.name": project_name, "sample.fma_body_site": sample_fma_body_site})
        return res

app.add_url_rule('/files', view_func=FilesAPI.as_view('files'), methods=['GET','OPTIONS'])

# Cases
class CasesAPI(MethodView):

    def get(self):
        res = jsonify({"project.name": project_name, "sample.fma_body_site": sample_fma_body_site})
        return res

    def options(self):
        res = jsonify({"project.name": project_name, "sample.fma_body_site": sample_fma_body_site})
        return res

app.add_url_rule('/cases', view_func=FilesAPI.as_view('cases'), methods=['GET','OPTIONS'])

app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True
    )
)

if __name__ == '__main__':
    app.run()
