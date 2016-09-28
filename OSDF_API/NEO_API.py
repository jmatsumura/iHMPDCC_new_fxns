from flask import Flask, jsonify
from flask import request
import copy
from py2neo import Graph

app = Flask(__name__)
neo4j_password = "osdf1"

graph = Graph(password=neo4j_password)
cypher = graph

providers = {"serverType": "neo4j", "url": "http://localhost:5000", "version": "1"}

annotations = []

sample_fma_body_site_str = {"description": "The Foundational Model of Anatomy Ontology term", "doc_type": "cases", 
                            "field": "fma_body_site", "full": "sample.fma_body_site", "type": "string"}

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

@app.route('/gql/_mapping', methods=['GET'])
def get_maps():
    res = jsonify({"sample.fma_body_site": sample_fma_body_site_str})
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

@app.route('/ui/search/summary', methods=['GET', 'OPTIONS', 'POST'])
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


if __name__ == '__main__':
    app.run(debug=True)