from flask import Flask, render_template, request,send_from_directory,send_file,redirect, make_response
from collections import Counter
from search import perform_search
from search_oop import perform_search_tt
from itertools import groupby
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from flasgger import Swagger
from datetime import datetime
import copy
import time
import requests
import os,shutil
import uuid
import json
import re
import base64
from datetime import datetime
from flask_cors import CORS, cross_origin
from utils import Utils as Ut
from load import init
import os
import time
import sys
from flask import abort, jsonify, g, url_for
from flask_httpauth import HTTPBasicAuth
import jwt
from werkzeug.security import generate_password_hash, check_password_hash
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import multiprocessing
from multiprocessing import Pool
from cognitive_search import bag_of_words

app = Flask(__name__)
CORS(app, support_credentials=True)
app.config['SECRET_KEY'] = 'the quick brown fox jumps over the lazy dog'


app.config['SWAGGER'] = {
    'title': 'Policy Intelligence Platform APIs',
    'openapi': "3.0.1",
    'uiversion': 3,
    'components': {
      'securitySchemes': {
        'basicAuth': {
          'type': 'http',
          'scheme': 'basic'
        }
      }
    },
    'security': [{
      'basicAuth': []
      }]
}

# extensions
auth = HTTPBasicAuth()

credentials={'username':'policyuser','password':'nf2020'}







highlight_re=re.compile("<em>(.*?)</em>")
word_re=re.compile("\b(\W+)\b")
search_result_global=dict()#global variable for results
document_map=dict()# global variable for holding the document to ID mapping


# initialization of environment file

env=init()
cosmo_endpoint=env['cosmo_endpoint']
cosmo_key=env['cosmo_key']
client = CosmosClient(cosmo_endpoint, cosmo_key)
database=client.get_database_client('policy-analysis')
saved_search_container=database.get_container_client(env['saved_search_container'])
assessment_report_container=database.get_container_client(env['assessment_report_container'])
trace_files_container=database.get_container_client(env['trace_files_container'])
trace_results_container=database.get_container_client(env['trace_results_container'])
version=env['version']
endpoint =env['azure_search_endpoint']
api_version = env['api_version']
headers = {'Content-Type': 'application/json',
        'api-key': env['api_key']}
user_name_env=env['user_name']
password_env=env['password']





def generate_auth_token(username, expires_in=600):
    return jwt.encode(
        {'id': username, 'exp': time.time() + expires_in},
        app.config['SECRET_KEY'], algorithm='HS256')

def verify_auth_token(token):
    try:

        data = jwt.decode(token, app.config['SECRET_KEY'],
                          algorithms=['HS256'])
    except Exception as err:
        return

    return True



@auth.verify_password
def verify_password(username_or_token, password):

    # first try to authenticate by token
    user = verify_auth_token(username_or_token)
    if not user:

        if username_or_token==user_name_env and password==password_env:
            return True
        else:
            return False
    return True


def key_func(k):
        return k['parent_key']
def key_func_result(k):
    return k['doc_name']



@app.route('/api/token')
@auth.login_required
@cross_origin(supports_credentials=True)

def get_auth_token():
    token = generate_auth_token(credentials['username'],600)
    return jsonify({'token': token.decode('ascii'), 'duration': 600})


@app.route('/api/auth')
@auth.login_required
@cross_origin(supports_credentials=True)
def auth_user():
    return jsonify({'status': True})


@app.route('/api/resource')
@auth.login_required
@cross_origin(supports_credentials=True)

def get_resource():
    return jsonify({'data': 'Hello world'})


@app.route('/markup/document', methods = ['POST'])
@auth.login_required
@cross_origin(support_credentials=True)
def getDocument():
    if request.method == 'POST':
        lob = None
        country = None
        documentType = None
        highlight_words = None
        highlight_words_def = None
        docname = None
        page_num = None
        phrase_list = None
        user_id = "123456"
        sublob = None
        section_bow = None
        data = request.get_json()

        if data:
            if 'name' in data:
                docname = data['name']
            if 'lob' in data:
                lob = data['lob']
            if 'sublob' in data:
                sublob = data['sublob']
            if 'highlightPhrases' in data:
                highlight_words = data['highlightPhrases']
            if 'phraseDefinitions' in data:
                highlight_words_def = data['phraseDefinitions']
            if 'country' in data:
                country = data['country']
            if 'phrases' in data:
                phrase_list = data['phrases']
            if 'pageNum' in data:
                page_num = data['pageNum']
            if "section_bow" in data:
                section_bow = data["section_bow"]
                section_bow = bag_of_words.get(section_bow,[])
            pdf=Ut.doc_pdf_highlighter(docname,lob,highlight_words,highlight_words_def,user_id,country,phrase_list,page_num,section_bow)
            encoded_string = base64.b64encode(pdf)
            return json.dumps({"data":str(encoded_string)})


        return response

@app.route('/trace/bubbleChart', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def getChartdata():

    data = {
        "lob":{
            "casualty": {
                "GB": 201,
                "ES": 140,
                "IE": 174,
                "FR": 166,
                "DE": 102,
                "IT": 99
            },
            "financial lines": {
                "GB": 133,
                "ES": 121,
                "IE": 88,
                "FR": 102,
                "DE": 101,
                "IT": 44
            },
            "fire": {
                "GB": 161,
                "ES": 101,
                "IE": 98,
                "FR": 102,
                "DE": 171,
                "IT": 141
            },
            "environmental":{
                "GB": 76,
                "ES": 111,
                "IE": 103,
                "FR": 98,
                "DE": 156,
                "IT": 122
            },
            "power": {
                "GB": 101,
                "ES": 141,
                "IE": 112,
                "FR": 151,
                "DE": 113,
                "IT": 122
            },
            "marine": {
                "GB": 88,
                "ES": 94,
                "IE": 107,
                "FR": 127,
                "DE": 119,
                "IT": 141
            }
        },
        "country":{
            "GB": {
                "casualty": 201,
                "financial lines": 133,
                "fire": 161,
                "environmental": 76,
                "power": 101,
                "marine": 88
            },
            "ES": {
                "casualty": 140,
                "financial lines": 121,
                "fire": 101,
                "environmental": 111,
                "power": 141,
                "marine": 94
            },
            "IE": {
                "casualty": 174,
                "financial lines": 88,
                "fire": 98,
                "environmental": 103,
                "power": 112,
                "marine": 107
            },
            "FR": {
                "casualty": 166,
                "financial lines": 102,
                "fire": 102,
                "environmental": 98,
                "power": 151,
                "marine": 127
            },
            "DE": {
                "casualty": 102,
                "financial lines": 101,
                "fire": 171,
                "environmental": 156,
                "power": 113,
                "marine": 119
            },
            "IT": {
                "casualty": 99,
                "financial lines": 44,
                "fire": 141,
                "environmental": 122,
                "power": 122,
                "marine": 141
            }
        }
    }

    return data

@app.route('/trace/endorsements/bubbleChart', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def getEndorsementChartdata():

    data = {
        "lob":{
            "casualty": {
                "GB": 201,
                "ES": 140,
                "IE": 174,
                "FR": 166,
                "DE": 102,
                "IT": 99
            },
            "financial lines": {
                "GB": 133,
                "ES": 121,
                "IE": 88,
                "FR": 102,
                "DE": 101,
                "IT": 44
            },
            "fire": {
                "GB": 161,
                "ES": 101,
                "IE": 98,
                "FR": 102,
                "DE": 171,
                "IT": 141
            },
            "environmental":{
                "GB": 76,
                "ES": 111,
                "IE": 103,
                "FR": 98,
                "DE": 156,
                "IT": 122
            },
            "power": {
                "GB": 101,
                "ES": 141,
                "IE": 112,
                "FR": 151,
                "DE": 113,
                "IT": 122
            },
            "marine": {
                "GB": 88,
                "ES": 94,
                "IE": 107,
                "FR": 127,
                "DE": 119,
                "IT": 141
            }
        },
        "country":{
            "GB": {
                "casualty": 201,
                "financial lines": 133,
                "fire": 161,
                "environmental": 76,
                "power": 101,
                "marine": 88
            },
            "ES": {
                "casualty": 140,
                "financial lines": 121,
                "fire": 101,
                "environmental": 111,
                "power": 141,
                "marine": 94
            },
            "IE": {
                "casualty": 174,
                "financial lines": 88,
                "fire": 98,
                "environmental": 103,
                "power": 112,
                "marine": 107
            },
            "FR": {
                "casualty": 166,
                "financial lines": 102,
                "fire": 102,
                "environmental": 98,
                "power": 151,
                "marine": 127
            },
            "DE": {
                "casualty": 102,
                "financial lines": 101,
                "fire": 171,
                "environmental": 156,
                "power": 113,
                "marine": 119
            },
            "IT": {
                "casualty": 99,
                "financial lines": 44,
                "fire": 141,
                "environmental": 122,
                "power": 122,
                "marine": 141
            }
        }
    }

    return data


@app.route('/trace/markup/endorsement', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def getHTMLEndorsement():

    result = {
      "title": "DocumentTitle",
      "id": "DocumentID",
      "clauseRefCode": "clauseRefCode",
      "section": [{
        "section": "categorySection",
        "htmlString": "HTML String of section",
        "sectionType": "SectionType",
        "matchType": "matchType",
        "endorsementPageNumber": 42,
        "policyDocumentPageNumber": 54
        }]
    }

    return json.dumps(result)

@app.route('/trace/markup', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def getHTMLDocument():

    documentId = request.args.get('id')
    wordingRefCode = request.args.get('refCode')
    documentId = documentId.replace('%20', ' ')

    query="SELECT * FROM c WHERE c.refCode ='{}' AND c.id = '{}'".format(wordingRefCode,documentId)

    items = list(trace_results_container.query_items(
        query=query,
        enable_cross_partition_query=True))


    result = {
        "section": []
    }

    for item in items:
        result["title"] = item['title']
        result["id"] = item['id']
        if len(item['matches']['exclusion']['results']) >= 1:
            for match in item['matches']['exclusion']['results']:
                if 'content' in match:
                    section = {
                        "section": match['section'],
                        "htmlString": match['content'],
                        "sectionType": 'exclusion',
                        "matchType": match['matchType']
                    }
                    if 'document_page' in match:
                        section['documentPageNumber'] = match['document_page']
                    if 'wording_page' in match:
                        section['wordingPageNumber'] = match['wording_page']
                    result['section'].append(section)
        if len(item['matches']['condition']['results']) >= 1:
            for match in item['matches']['condition']['results']:
                if 'content' in match:
                    section = {
                        "section": match['section'],
                        "htmlString": match['content'],
                        "sectionType": 'condition',
                        "matchType": match['matchType']
                    }
                    if 'document_page' in match:
                        section['documentPageNumber'] = match['document_page']
                    if 'wording_page' in match:
                        section['wordingPageNumber'] = match['wording_page']
                    result['section'].append(section)
        if len(item['matches']['extension']['results']) >= 1:
            for match in item['matches']['extension']['results']:
                if 'content' in match:
                    section = {
                        "section": match['section'],
                        "htmlString": match['content'],
                        "sectionType": 'extension',
                        "matchType": match['matchType']
                    }
                    if 'document_page' in match:
                        section['documentPageNumber'] = match['document_page']
                    if 'wording_page' in match:
                        section['wordingPageNumber'] = match['wording_page']
                    result['section'].append(section)
        if len(item['matches']['definition']['results']) >= 1:
            for match in item['matches']['definition']['results']:
                if 'content' in match:
                    section = {
                        "section": match['section'],
                        "htmlString": match['content'],
                        "sectionType": 'definition',
                        "matchType": match['matchType']
                    }
                    if 'document_page' in match:
                        section['documentPageNumber'] = match['document_page']
                    if 'wording_page' in match:
                        section['wordingPageNumber'] = match['wording_page']
                    result['section'].append(section)
        if len(item['matches']['covered section']['results']) >= 1:
            for match in item['matches']['covered section']['results']:
                if 'content' in match:
                    section = {
                        "section": match['section'],
                        "htmlString": match['content'],
                        "sectionType": 'covered section',
                        "matchType": match['matchType']
                    }
                    if 'document_page' in match:
                        section['documentPageNumber'] = match['document_page']
                    if 'wording_page' in match:
                        section['wordingPageNumber'] = match['wording_page']
                    result['section'].append(section)
        if len(item['matches']['misc']['results']) >= 1:
            for match in item['matches']['misc']['results']:
                if 'content' in match:
                    section = {
                        "section": match['section'],
                        "htmlString": match['content'],
                        "sectionType": 'miscellaneous',
                        "matchType": match['matchType']
                    }
                    if 'document_page' in match:
                        section['documentPageNumber'] = match['document_page']
                    if 'wording_page' in match:
                        section['wordingPageNumber'] = match['wording_page']
                    result['section'].append(section)

    return json.dumps(result)


#@app.errorhandler(Exception)
#def server_error(err):
#    app.logger.error(err)
#    return "Server Error: " + app.logger.error(err), 500

# @app.errorhandler(Exception)
# def path_error(err):
#     app.logger.error(err)
#     return "Server Error: " + str(err), 400

@app.route('/trace/analytics/endorsements', methods= ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def getAnalyticsEndorsements():

    result = {
        "batchTime": "2021-04-01 14:13:42",
        "full": {
            "total": 104
        },
        "noMatch": {
            "section": {
                "conditions": 38,
                "covered": 38,
                "definitions": 39,
                "exclusions": 39,
                "extensions": 6,
                "endorsements": 7,
                "miscellaneous": 0
            },
            "total": 41
        },
        "partial": {
            "relevance": {
                "high": 14,
                "low": 4,
                "medium": 7
            },
            "section": {
                "conditions": 4,
                "covered": 0,
                "definitions": 4,
                "exclusions": 6,
                "extensions": 8,
                "endorsements": 6,
                "miscellaneous": 17
            },
            "total": 25
        },
        "refCodes": {
            "MN MasterPackage 2019": {
            "count": 5,
            "wordingTitle": "MasterPackage Multinational-UK-2019 (Limit of Loss S 1 & 2)-SPECIMEN-(to 31-12-2020)"
            }
        },
        "endorsementWordings": {
            "endorsementWording": {
                "count": 1
            }
        },
        "total": 170
    }

    return json.dumps(result)

@app.route('/trace/analytics', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def getAnalytics():

    # We only need country and lob

    country = request.args.get('country')
    lob = request.args.get('lob')

    fullCount = 0
    partialCount = 0
    noCount = 0
    coveredCount = 0
    exclusionCount = 0
    conditionCount = 0
    extensionCount = 0
    definitionCount = 0
    miscCount = 0
    coveredCountNoMatch = 0
    exclusionCountNoMatch = 0
    conditionCountNoMatch = 0
    extensionCountNoMatch = 0
    definitionCountNoMatch = 0
    miscCountNoMatch = 0
    lowCount = 0
    mediumCount = 0
    highCount = 0
    batchTime = datetime.now()
    btStr = batchTime.strftime("%Y-%m-%d %H:%M:%S")

    query="SELECT * FROM c WHERE c.country = '{}' AND c.lob = '{}'".format(country,lob)

    items = list(trace_results_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    refCodes = {}

    for item in items:
        if len(refCodes) < 1 and item['matchType'].lower() != 'full':
            refCodes[item['refCode']] = {'count': 0, 'wordingTitle': item['wordingTitle']}
        if item['refCode'] not in refCodes and item['matchType'].lower() != 'full':
            refCodes[item['refCode']] = {'count': 0, 'wordingTitle': item['wordingTitle']}
        if item['matchType'].lower() == 'full':
            fullCount = fullCount + 1
        elif item['matchType'].lower() == 'partial':
            partialCount = partialCount + 1
            refCodes[item['refCode']]['count'] = refCodes.get(item['refCode']).get('count',0) + 1
            if item['relevance'].lower() == 'low':
                lowCount = lowCount + 1
            elif item['relevance'].lower() == 'medium':
                mediumCount = mediumCount + 1
            elif item['relevance'].lower() == 'high':
                highCount = highCount + 1
            if len(item['matches']['exclusion']['results']) >= 1:
                for match in item['matches']['exclusion']['results']:
                    if match['matchType'].lower() == 'partial':
                        exclusionCount = exclusionCount + 1
                        break;
            if len(item['matches']['condition']['results']) >= 1:
                for match in item['matches']['condition']['results']:
                    if match['matchType'].lower() == 'partial':
                        conditionCount = conditionCount + 1
                        break;
            if len(item['matches']['extension']['results']) >= 1:
                for match in item['matches']['extension']['results']:
                    if match['matchType'].lower() == 'partial':
                        extensionCount = extensionCount + 1
                        break;
            if len(item['matches']['definition']['results']) >= 1:
                for match in item['matches']['definition']['results']:
                    if match['matchType'].lower() == 'partial':
                        definitionCount = definitionCount + 1
                        break;
            if len(item['matches']['covered section']['results']) >= 1:
                for match in item['matches']['covered section']['results']:
                    if match['matchType'].lower() == 'partial':
                        coveredCount = coveredCount + 1
                        break;
            if len(item['matches']['misc']['results']) >= 1:
                for match in item['matches']['misc']['results']:
                    if match['matchType'].lower() == 'partial':
                        miscCount = miscCount + 1
                        break;
        elif item['matchType'].lower() == 'no':
            noCount = noCount + 1
            refCodes[item['refCode']]['count'] = refCodes.get(item['refCode']).get('count',0) + 1
            if len(item['matches']['exclusion']['results']) >= 1:
                for match in item['matches']['exclusion']['results']:
                    if match['matchType'].lower() == 'no':
                        exclusionCountNoMatch = exclusionCountNoMatch + 1
                        break;
            if len(item['matches']['condition']['results']) >= 1:
                for match in item['matches']['condition']['results']:
                    if match['matchType'].lower() == 'no':
                        conditionCountNoMatch = conditionCountNoMatch + 1
                        break;
            if len(item['matches']['extension']['results']) >= 1:
                for match in item['matches']['extension']['results']:
                    if match['matchType'].lower() == 'no':
                        extensionCountNoMatch = extensionCountNoMatch + 1
                        break;
            if len(item['matches']['definition']['results']) >= 1:
                for match in item['matches']['definition']['results']:
                    if match['matchType'].lower() == 'no':
                        definitionCountNoMatch = definitionCountNoMatch + 1
                        break;
            if len(item['matches']['covered section']['results']) >= 1:
                for match in item['matches']['covered section']['results']:
                    if match['matchType'].lower() == 'no':
                        coveredCountNoMatch = coveredCountNoMatch + 1
                        break;
            if len(item['matches']['misc']['results']) >= 1:
                for match in item['matches']['misc']['results']:
                    if match['matchType'].lower() == 'no':
                        miscCountNoMatch = miscCountNoMatch + 1
                        break;

    analytics = {
		"full": {
			"total": fullCount
		},
		"partial": {
			"section": {
				"covered": coveredCount,
				"exclusions": exclusionCount,
				"conditions": conditionCount,
				"extensions": extensionCount,
				"definitions": definitionCount,
				"miscellaneous": miscCount
			},
			"relevance": {
				"low": lowCount,
				"medium": mediumCount,
				"high": highCount,
			},
			"total": partialCount
        },
		"total": fullCount + partialCount,
        "refCodes": refCodes,
        "batchTime":btStr
    }
    return analytics


@app.route('/search/documents', methods = ['POST'])
@auth.login_required
@cross_origin(supports_credentials=True)
def searchDocuments():
    data = request.get_json()

    searchValues = None
    searchString = None
    LOB = None
    country = None
    documentType = None
    language = None
    effectiveDateFrom = None
    effectiveDateTo = None
    SLOB = None
    documentSection = None
    substituteDefinition = None
    expandedSearch = None

    if data:
        if 'query' in data:
            searchValues = data['query']
        if 'queryString' in data:
            searchString = data['queryString']
        if 'lob' in data:
            LOB = data['lob']
        if 'country' in data:
            country = data['country']
        if 'documentType' in data:
            documentType = data['documentType']
        if 'language' in data:
            language = data['language']
        if 'fromDate' in data:
            effectiveDateFrom = data['fromDate']
        if 'toDate' in data:
            effectiveDateTo = data['toDate']
        if 'sublob' in data:
            SLOB = data['sublob']
        if 'section' in data:
            documentSection = data['section']
        if 'substituteDefinition' in data:
            substituteDefinition = data['substituteDefinition']
        if 'expandedSearch' in data:
            expandedSearch = data['expandedSearch']



    result = perform_search_tt(_pool,searchValues,searchString,LOB,country,documentType,
        language,effectiveDateFrom,effectiveDateTo,SLOB,documentSection,
        substituteDefinition,expandedSearch)

    return json.dumps(result)


@app.route('/clear_files',methods = ['POST','GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def clear_files():
    delete_contents('out')
    delete_contents('click_tag')

    return "Cleared files"

@app.route('/trace/download', methods=['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def downloadDoc():
    documentType = request.args.get('docType')
    title = request.args.get('title')
    country = request.args.get('country')
    lob = request.args.get('lob')
    title = title.replace('%20',' ')

    blobServiceClient = BlobServiceClient.from_connection_string(env['blob_connection_string'])
    pipDocsContainerClient = blobServiceClient.get_container_client(env['container_name_documents'])

    if documentType == 'policy':
        blobId = country.upper() + '-' + lob.capitalize() + '/' + title + '.pdf'
    elif documentType == 'wording':
        blobId = country.upper() + '-' + lob.capitalize() + '-Wordings/' + title + '.pdf'

    try:
        print(blobId)
        blob_list = []
        for blob in pipDocsContainerClient.list_blobs():
            print(blob.name)
        data = pipDocsContainerClient.download_blob(blobId)
        test = data.readall()
        response = make_response(test)
        response.headers["Content-Type"] = "application/pdf"
        response.headers["Content-Disposition"] = "inline; filename=output.pdf"
    except Exception as ex:
        response = "The document was not found."
    return response

@app.route('/trace/endorsements/download', methods=['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def downloadEndorsementDoc():
    documentType = request.args.get('docType')
    title = request.args.get('title')
    country = request.args.get('country')
    lob = request.args.get('lob')
    title = title.replace('%20',' ')

    blobServiceClient = BlobServiceClient.from_connection_string(env['blob_connection_string'])
    pipDocsContainerClient = blobServiceClient.get_container_client(env['container_name_documents'])

    if documentType == 'policy':
        blobId = country.upper() + '-' + lob.capitalize() + '/' + title + '.pdf'
    elif documentType == 'endorsement':
        blobId = country.upper() + '-' + lob.capitalize() + '-Endorsements/' + title + '.pdf'

    try:
        print(blobId)
        blob_list = []
        for blob in pipDocsContainerClient.list_blobs():
            print(blob.name)
        data = pipDocsContainerClient.download_blob(blobId)
        test = data.readall()
        response = make_response(test)
        response.headers["Content-Type"] = "application/pdf"
        response.headers["Content-Disposition"] = "inline; filename=output.pdf"
    except Exception as ex:
        response = "The document was not found."
    return response

@app.route('/trace/search/basewordings', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def searchBaseWordings():

    lob = request.args.get('lob').lower()
    country = request.args.get('country').upper()
    documentType = request.args.get('docType')
    language = request.args.get('language')
    effectiveDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    sublob = request.args.get('sublob')
    usage = request.args.get('usage')
    segment = request.args.get('segment')

    result = {
        "filters": {
            "country": country,
            "docType": documentType,
            "endDate": endDate,
            "language": language,
            "startDate": effectiveDate,
            "sublob": sublob,
            "lob": lob,
            "usage": usage,
            "segment": segment
        },
        "results": []
    }

    query="SELECT c.refCode, c.title FROM c WHERE c.file_type = 'base wording' AND c.effectiveDate >='{}' AND c.effectiveDate <= '{}' AND c.country ='{}' AND c.lob='{}'".format(effectiveDate,endDate,country,lob)

#    if documentType is not None:
#        query += " AND c.file_type = '{}'".format(documentType)
#    if language is not None:
#        query += " AND c.language = '{}'".format(language)
#    if expiryDate is not None:
#        query="SELECT c.refCode, c.title FROM c WHERE c.effectiveDate >='{}' AND c.effectiveDate <= '{}' AND c.country ='{}' AND c.lob='{}'".format(effectiveDate,expiryDate,country,lob)
#    if sublob is not None:
#        query += " AND c.sublob = '{}'".format(sublob)
#    if usage is not None:
#        query += " AND c.usage = '{}'".format(usage)
#    if segment is not None:
#        query += " AND c.segment = '{}'".format(segment)

    print(query)

    items = list(trace_files_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    for item in items:
        result['results'].append(item)

    return json.dumps(result)

def createBaseWordingResponse(refCode):
    #Everything below this should be its own function.
    fullCount = 0
    partialCount = 0
    noCount = 0
    sections = []
    miscCount = 0
    coveredCount = 0
    conditionCount = 0
    extensionCount = 0
    definitionCount = 0
    exclusionCount = 0
    miscCountLow = 0
    coveredCountLow = 0
    conditionCountLow = 0
    extensionCountLow = 0
    definitionCountLow = 0
    exclusionCountLow = 0
    miscCountMed = 0
    coveredCountMed = 0
    conditionCountMed = 0
    extensionCountMed = 0
    definitionCountMed = 0
    exclusionCountMed = 0
    miscCountHigh = 0
    coveredCountHigh = 0
    conditionCountHigh = 0
    extensionCountHigh = 0
    definitionCountHigh = 0
    exclusionCountHigh = 0
    coveredCountNoMatch = 0
    exclusionCountNoMatch = 0
    conditionCountNoMatch = 0
    extensionCountNoMatch = 0
    definitionCountNoMatch = 0
    miscCountNoMatch = 0
    docLow = 0
    docMedium = 0
    docHigh = 0


    query="SELECT * FROM c WHERE c.refCode ='{}'".format(refCode)

    items = list(trace_results_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    for item in items:
        res = {
            "policyDetails": {
                "policyNo": item['policyDetails']['policyNo'],
                "expiryDate": item['policyDetails']['expiryDate'],
                "effectiveDate": item['policyDetails']['effectiveDate']
            },
            "title": item['title'],
            "id": item['id'],
            "matchType": item['matchType'],
            "relevance": item['relevance'],
            "refCode": item['refCode'],
            "matches": {
                "covered": {},
                "exclusions": {},
                "conditions": {},
                "extensions": {},
                "definitions": {},
                "miscellaneous": {}
            }
        }
        if item['matchType'].lower() == 'full':
            fullCount = fullCount + 1
        elif item['matchType'].lower() == 'partial':
            partialCount = partialCount + 1
            if item['relevance'].lower() == 'low':
                docLow = docLow + 1
            elif item['relevance'].lower() == 'medium':
                docMedium = docMedium + 1
            elif item['relevance'].lower() == 'high':
                docHigh = docHigh + 1
            if len(item['matches']['exclusion']['results']) >= 1:
                res['matches']['exclusions']['matchType'] = item['matches']['exclusion']['matchType']
                res['matches']['exclusions']['relevance'] = item['matches']['exclusion']['relevance']
                if item['matches']['exclusion']['relevance'].lower() == 'low':
                    exclusionCountLow = exclusionCountLow + 1
                elif item['matches']['exclusion']['relevance'].lower() == 'medium':
                    exclusionCountMed = exclusionCountMed + 1
                elif item['matches']['exclusion']['relevance'].lower() == 'high':
                    exclusionCountHigh = exclusionCountHigh + 1
                for match in item['matches']['exclusion']['results']:
                    if match['matchType'].lower() == 'partial':
                        exclusionCount = exclusionCount + 1
                        break;
            if len(item['matches']['condition']['results']) >= 1:
                res['matches']['conditions']['matchType'] = item['matches']['condition']['matchType']
                res['matches']['conditions']['relevance'] = item['matches']['condition']['relevance']
                if item['matches']['condition']['relevance'].lower() == 'low':
                    conditionCountLow = conditionCountLow + 1
                elif item['matches']['condition']['relevance'].lower() == 'medium':
                    conditionCountMed = conditionCountMed + 1
                elif item['matches']['condition']['relevance'].lower() == 'high':
                    conditionCountHigh = conditionCountHigh + 1
                for match in item['matches']['condition']['results']:
                    if match['matchType'].lower() == 'partial':
                        conditionCount = conditionCount + 1
                        break;
            if len(item['matches']['extension']['results']) >= 1:
                res['matches']['extensions']['matchType'] = item['matches']['extension']['matchType']
                res['matches']['extensions']['relevance'] = item['matches']['extension']['relevance']
                if item['matches']['extension']['relevance'].lower() == 'low':
                    extensionCountLow = extensionCountLow + 1
                elif item['matches']['extension']['relevance'].lower() == 'medium':
                    extensionCountMed = extensionCountMed + 1
                elif item['matches']['extension']['relevance'].lower() == 'high':
                    extensionCountHigh = extensionCountHigh + 1
                for match in item['matches']['extension']['results']:
                    if match['matchType'].lower() == 'partial':
                        extensionCount = extensionCount + 1
                        break;
            if len(item['matches']['definition']['results']) >= 1:
                res['matches']['definitions']['matchType'] = item['matches']['definition']['matchType']
                res['matches']['definitions']['relevance'] = item['matches']['definition']['relevance']
                if item['matches']['definition']['relevance'].lower() == 'low':
                    definitionCountLow = definitionCountLow + 1
                elif item['matches']['definition']['relevance'].lower() == 'medium':
                    definitionCountMed = definitionCountMed + 1
                elif item['matches']['definition']['relevance'].lower() == 'high':
                    definitionCountHigh = definitionCountHigh + 1
                for match in item['matches']['definition']['results']:
                    if match['matchType'].lower() == 'partial':
                        definitionCount = definitionCount + 1
                        break;
            if len(item['matches']['covered section']['results']) >= 1:
                res['matches']['covered']['matchType'] = item['matches']['covered section']['matchType']
                res['matches']['covered']['relevance'] = item['matches']['covered section']['relevance']
                if item['matches']['covered section']['relevance'].lower() == 'low':
                    coveredCountLow = coveredCountLow + 1
                elif item['matches']['covered section']['relevance'].lower() == 'medium':
                    coveredCountMed = coveredCountMed + 1
                elif item['matches']['covered section']['relevance'].lower() == 'high':
                    coveredCountHigh = coveredCountHigh + 1
                for match in item['matches']['covered section']['results']:
                    if match['matchType'].lower() == 'partial':
                        coveredCount = coveredCount + 1
                        break;
            if len(item['matches']['misc']['results']) >= 1:
                res['matches']['miscellaneous']['matchType'] = item['matches']['misc']['matchType']
                res['matches']['miscellaneous']['relevance'] = item['matches']['misc']['relevance']
                if item['matches']['misc']['relevance'].lower() == 'low':
                    miscCountLow = miscCountLow + 1
                elif item['matches']['misc']['relevance'].lower() == 'medium':
                    miscCountMed = miscCountMed + 1
                elif item['matches']['misc']['relevance'].lower() == 'high':
                    miscCountHigh = miscCountHigh + 1
                for match in item['matches']['misc']['results']:
                    if match['matchType'].lower() == 'partial':
                        miscCount = miscCount + 1
                        break;
            res['matches']['exclusions']['total'] = exclusionCount
            res['matches']['conditions']['total'] = conditionCount
            res['matches']['extensions']['total'] = extensionCount
            res['matches']['definitions']['total'] = definitionCount
            res['matches']['covered']['total'] = coveredCount
            res['matches']['miscellaneous']['total'] = miscCount
        elif item['matchType'].lower() == 'no':
            noCount = noCount + 1
            if len(item['matches']['exclusion']['results']) >= 1:
                res['matches']['exclusions']['matchType'] = item['matches']['exclusion']['matchType']
                res['matches']['exclusions']['relevance'] = item['matches']['exclusion']['relevance']
                for match in item['matches']['exclusion']['results']:
                    if match['matchType'].lower() == 'no' or match['matchType'].lower() == 'partial':
                        exclusionCountNoMatch = exclusionCountNoMatch + 1
            if len(item['matches']['condition']['results']) >= 1:
                res['matches']['conditions']['matchType'] = item['matches']['condition']['matchType']
                res['matches']['conditions']['relevance'] = item['matches']['condition']['relevance']
                for match in item['matches']['condition']['results']:
                    if match['matchType'].lower() == 'no' or match['matchType'].lower() == 'partial':
                        conditionCountNoMatch = conditionCountNoMatch + 1
            if len(item['matches']['extension']['results']) >= 1:
                res['matches']['extensions']['matchType'] = item['matches']['extension']['matchType']
                res['matches']['extensions']['relevance'] = item['matches']['extension']['relevance']
                for match in item['matches']['extension']['results']:
                    if match['matchType'].lower() == 'no' or match['matchType'].lower() == 'partial':
                        extensionCountNoMatch = extensionCountNoMatch + 1
            if len(item['matches']['definition']['results']) >= 1:
                res['matches']['definitions']['matchType'] = item['matches']['definition']['matchType']
                res['matches']['definitions']['relevance'] = item['matches']['definition']['relevance']
                for match in item['matches']['definition']['results']:
                    if match['matchType'].lower() == 'no' or match['matchType'].lower() == 'partial':
                        definitionCountNoMatch = definitionCountNoMatch + 1
            if len(item['matches']['covered section']['results']) >= 1:
                res['matches']['covered']['matchType'] = item['matches']['covered section']['matchType']
                res['matches']['covered']['relevance'] = item['matches']['covered section']['relevance']
                for match in item['matches']['covered section']['results']:
                    if match['matchType'].lower() == 'no' or match['matchType'].lower() == 'partial':
                        coveredCountNoMatch = coveredCountNoMatch + 1
            if len(item['matches']['misc']['results']) >= 1:
                res['matches']['miscellaneous']['matchType'] = item['matches']['misc']['matchType']
                res['matches']['miscellaneous']['relevance'] = item['matches']['misc']['relevance']
                for match in item['matches']['misc']['results']:
                    if match['matchType'].lower() == 'no' or match['matchType'].lower() == 'partial':
                        miscCountNoMatch = miscCountNoMatch + 1
            res['matches']['exclusions']['total'] = exclusionCountNoMatch
            res['matches']['conditions']['total'] = conditionCountNoMatch
            res['matches']['extensions']['total'] = extensionCountNoMatch
            res['matches']['definitions']['total'] = definitionCountNoMatch
            res['matches']['covered']['total'] = coveredCountNoMatch
            res['matches']['miscellaneous']['total'] = miscCountNoMatch
        sections.append(res)

    result = {
	"abstract": {
		"full": {
			"total": fullCount
		},
		"partial": {
			"section": {
				"covered": {
        			"relevance": {
        				"low": {
                            "total": coveredCountLow
        				},
        				"medium": {
                            "total": coveredCountMed
        				},
        				"high": {
                            "total": coveredCountHigh
        				}
        			},
					"total": coveredCount
				},
				"exclusions": {
        			"relevance": {
        				"low": {
                            "total": exclusionCountLow
        				},
        				"medium": {
                            "total": exclusionCountMed
        				},
        				"high": {
                            "total": exclusionCountHigh
        				}
        			},
					"total": exclusionCount
				},
				"conditions": {
        			"relevance": {
        				"low": {
                            "total": conditionCountLow
        				},
        				"medium": {
                            "total": conditionCountMed
        				},
        				"high": {
                            "total": conditionCountHigh
        				}
        			},
					"total": conditionCount
				},
				"extensions": {
        			"relevance": {
        				"low": {
                            "total": extensionCountLow
        				},
        				"medium": {
                            "total": extensionCountMed
        				},
        				"high": {
                            "total": extensionCountHigh
        				}
        			},
					"total": extensionCount
				},
				"definitions": {
        			"relevance": {
        				"low": {
                            "total": definitionCountLow
        				},
        				"medium": {
                            "total": definitionCountMed
        				},
        				"high": {
                            "total": definitionCountHigh
        				}
        			},
					"total": definitionCount
				},
				"miscellaneous": {
        		      "relevance": {
                        "low": {
                            "total": miscCountLow
        				},
        				"medium": {
                            "total": miscCountMed
        				},
        				"high": {
                            "total": miscCountHigh
        				}
        			},
					"total": miscCount
				}
			},
			"relevance": {
				"low": {
                    "covered": {
                        "total": coveredCountLow
                    },
                    "exclusions": {
                        "total": exclusionCountLow
                    },
                    "conditions": {
                        "total": conditionCountLow
                    },
                    "extensions": {
                        "total": extensionCountLow
                    },
                    "definitions": {
                        "total": definitionCountLow
                    },
                    "miscellaneous": {
                        "total": miscCountLow
                    },
                    "total": docLow
				},
				"medium": {
                    "covered": {
                        "total": coveredCountMed
                    },
                    "exclusions": {
                        "total": exclusionCountMed
                    },
                    "conditions": {
                        "total": conditionCountMed
                    },
                    "extensions": {
                        "total": extensionCountMed
                    },
                    "definitions": {
                        "total": definitionCountMed
                    },
                    "miscellaneous": {
                        "total": miscCountMed
                    },
                    "total": docMedium
				},
				"high": {
                    "covered": {
                        "total": coveredCountHigh
                    },
                    "exclusions": {
                        "total": exclusionCountHigh
                    },
                    "conditions": {
                        "total": conditionCountHigh
                    },
                    "extensions": {
                        "total": extensionCountHigh
                    },
                    "definitions": {
                        "total": definitionCountHigh
                    },
                    "miscellaneous": {
                        "total": miscCountHigh
                    },
                    "total": docHigh
				}
			},
			"total": partialCount
		},
		"total": fullCount + partialCount
	}
}

    result['results'] = sections

    return result



@app.route('/trace/results/basewordings', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def viewBaseWordings():
    wordingRefCode = request.args.get('refCode')

    result = createBaseWordingResponse(wordingRefCode)

    return json.dumps(result)

@app.route('/trace/results/endorsements', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def viewResultsEndorsements():
    #wordingRefCode = request.args.get('refCode')

    #result = createBaseWordingResponse(wordingRefCode)
    result = {
        "abstract": {
            "full": {
                "total": 75
            },
            "partial": {
                "section": {
                    "covered": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 0
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 0
                    },
                    "exclusions": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 0
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 0
                    },
                    "conditions": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 0
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 0
                    },
                    "extensions": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 0
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 0
                    },
                    "definitions": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 0
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 0
                    },
                    "endorsements": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 0
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 0
                    },
                    "miscellaneous": {
                        "relevance": {
                            "low": {
                                "total": 0
                            },
                            "medium": {
                                "total": 1
                            },
                            "high": {
                                "total": 0
                            }
                        },
                        "total": 1
                    }
                },
                "relevance": {
                    "low": {
                        "covered": {
                            "total": 0
                        },
                        "exclusions": {
                            "total": 0
                        },
                        "conditions": {
                            "total": 0
                        },
                        "extensions": {
                            "total": 0
                        },
                        "definitions": {
                            "total": 0
                        },
                        "endorsements": {
                            "total": 0
                        },
                        "miscellaneous": {
                            "total": 0
                        },
                        "total": 0
                    },
                    "medium": {
                        "covered": {
                            "total": 0
                        },
                        "exclusions": {
                            "total": 0
                        },
                        "conditions": {
                            "total": 0
                        },
                        "extensions": {
                            "total": 0
                        },
                        "definitions": {
                            "total": 0
                        },
                        "endorsements": {
                            "total": 0
                        },
                        "miscellaneous": {
                            "total": 1
                        },
                        "total": 1
                    },
                    "high": {
                        "covered": {
                            "total": 0
                        },
                        "exclusions": {
                            "total": 0
                        },
                        "conditions": {
                            "total": 0
                        },
                        "extensions": {
                            "total": 0
                        },
                        "definitions": {
                            "total": 0
                        },
                        "endorsements": {
                            "total": 0
                        },
                        "miscellaneous": {
                            "total": 0
                        },
                        "total": 0
                    }
                },
                "total": 1
            },
            "no": {
                "section": {
                    "covered": {
                        "total": 0
                    },
                    "exclusions": {
                        "total": 0
                    },
                    "conditions": {
                        "total": 0
                    },
                    "extensions": {
                        "total": 0
                    },
                    "definitions": {
                        "total": 0
                    },
                    "endorsements": {
                        "total": 0
                    },
                    "miscellaneous": {
                        "total": 0
                    }
                },
                "total": 0
            },
            "total": 4
        },
        "results": [{
            "policyDetails": {
                "policyNo": "98107865",
                "expiryDate": "2021-10-30",
                "effectiveDate": "2020-10-30"
            },
            "title": "Chubb Midmarket Liability Policy Wording (Chubb02-125-0218)",
            "id": "Chubb PDBI - UK - 04-2018 (SE - from 01-01-2019 to 31-12-20) - SPECIMENREDACTED_B3934012-921B-4D9B-8775-6E4112413588",
            "matchType": "partial",
            "relevance": "High",
            "clauseRefCode": "clauseRefCode",
            "wordingRefCode": "PDBI 01/01/2020",
            "documentType": "Endorsement",
            "matches": {
                "covered": {
                    "matchType": "full",
                    "relevance": "",
                    "total": 0
                },
                "exclusions": {
                    "matchType": "full",
                    "relevance": "",
                    "total": 0
                },
                "conditions": {
                    "matchType": "full",
                    "relevance": "",
                    "total": 0
                },
                "extensions": {
                    "matchType": "full",
                    "relevance": "",
                    "total": 0
                },
                "definitions": {
                    "matchType": "full",
                    "relevance": "",
                    "total": 0
                },
                "endorsements": {
                    "matchType": "full",
                    "relevance": "",
                    "total": 0
                },
                "miscellaneous": {
                    "matchType": "partial",
                    "relevance": "medium",
                    "total": 1
                }
            }
        }]
    }

    return json.dumps(result)

@app.route('/trace/search/endorsements', methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def viewEndorsements():
    #wordingRefCode = request.args.get('refCode')

    #result = createBaseWordingResponse(wordingRefCode)

    result = {
        "filters": {
            "country": "GB",
            "docType": "null",
            "startDate": "2020-01-01",
            "endDate": "2021-01-01",
            "query": "Endorsement search query",
            "language": "null",
            "sublob": "null",
            "lob": "fire",
            "usage": "null",
            "segment": "null"
        },
        "results": [{
            "title": "Chubb Midmarket Liability Policy Wording (Chubb02-125-0218)",
            "RefCode": "EndorsementRefCode"
        }],
        "wordingRefCodes": []
    }

    return json.dumps(result)

@app.route('/save_search',methods = ['POST','GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def save_search():

    save_dict=dict()



    countries=request.args.get('country').split(',')
    parent_key=str(uuid.uuid4())

    api_payload=request.json

    search_string=request.args.get('search_string')
    lob=request.args.get('lob').lower()
    language=request.args.get('language','english').lower()
    doc_collection=request.args.get('doc_collection',None)
    def_sub_search=True if request.args.get('def_sub_search') == 'true' else False
    user_id=request.args.get('user_id','no_user_id')

    word_proximity_limit=int(request.args.get('word_proximity_limit',50))
    save_type=request.args.get('save_type')




    section=request.args.get('section',None)
    is_exclusion=True if section == 'exclusion' else False
    is_extension=True if section == 'extension' else False
    is_condition=True  if section == 'condition' else False
    is_definition=True  if section == 'definition' else False
    is_covered=True if  section == 'covered' else False


    sub_type=request.args.get('sub_type',None)
    is_endorsement=True  if sub_type == 'endorsement' else False
    is_basewording=True  if sub_type == 'base wording' else False
    is_expanded=True if request.args.get('is_expanded') == 'true' else False

    super_list=request.json['search_payload']


    category=request.args.get('category','default category')
    category_id=request.args.get('category_id','0')

    description=request.args.get('description')
    criteria_name=request.args.get('criteria_name','no name given')
    payload=api_payload["search_payload"]
    search_keywords=api_payload["search_keywords"]


    type_of_doc=request.args.get('type_of_doc','wordings').lower()
    search_index='{}-{}-{}'.format(lob,type_of_doc,version)
    for country in countries:

        count=Ut.incremental_count(lob,country,saved_search_container)

        save_dict['id']=str(uuid.uuid4())
        save_dict['parent_key']=parent_key
        save_dict['inremental_count']=count
        save_dict['inremental_id']="{}-{}-{}-{}".format(country[:2].upper(),lob[:3].upper(),type_of_doc.upper(),count)

        save_dict['search_string']=search_string
        save_dict['doc_collection']=doc_collection
        save_dict['lob']=lob
        save_dict['country']=country
        save_dict['countries_in_standard']=countries
        save_dict['language']=language
        save_dict['def_sub_search'] = def_sub_search
        save_dict['user_id']=user_id
        save_dict['word_proximity_limit']=word_proximity_limit

        save_dict['is_exclusion']=is_exclusion
        save_dict['is_extension']=is_extension
        save_dict['is_covered']=is_covered
        save_dict['is_condition']=is_condition
        save_dict['is_definition']=is_definition

        save_dict['is_endorsement']=is_endorsement
        save_dict['is_basewording']=is_basewording
        save_dict['is_expanded']=is_expanded

        if section:
            save_dict['section']=section.split(',')
        else:
            save_dict['section']=[]
        if sub_type:
            save_dict['sub_type']=sub_type.split(',')
        else:
            save_dict['sub_type']=[]

        if doc_collection:
            save_dict['wording_type']=doc_collection.split(',')
        else:
            save_dict['wording_type']=[]









        save_dict['super_list']=super_list




        save_dict['category']=category
        save_dict['category_id']=category_id

        #save_category
        save_dict['description']=description
        save_dict['criteria_name']=criteria_name
        save_dict['payload']=payload
        save_dict['search_keywords']=search_keywords


        save_dict['type_of_doc']=type_of_doc
        save_dict['search_index']=search_index
        save_dict['save_type']=save_type
        # print(save_dict)



        try:
            saved_search_container.create_item(body=save_dict)

        except Exception as err:
            return json.dumps({"status":"failed",'error':err})
    return json.dumps({"status":"success","message":"saved {} standard(s)".format(len(countries))})



@app.route('/delete_saved_searches',methods = ['POST','GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def delete_saved_searches():
    parent_key=request.args.get('parent_key')

    query="SELECT * FROM c WHERE c.parent_key ='{}'".format(parent_key)

    items = list(saved_search_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    #deletion of standards when the coutry list is updated

    delet_countries=[i['id'] for i in items]
    for id in delet_countries:
        query = "SELECT * FROM c WHERE c.id ='{}'".format(id)
        try:
            for item in saved_search_container.query_items(query,enable_cross_partition_query=True):
                saved_search_container.delete_item(item, partition_key=str(id))
            for item in assessment_report_container.query_items(query,enable_cross_partition_query=True):
                assessment_report_container.delete_item(item, partition_key=str(id))

        except Exception as err:
            return json.dumps({"status":"failed",'error':err})

    return json.dumps({"status":"success","status":"success",'message':'deleted {} standards'.format(len(delet_countries))})


@app.route('/update_search',methods = ['POST','GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def update_search():



    countries=request.args.get('country').split(',')
    save_dict=dict()
    #unique id for cosmosdb
    api_payload=request.json

    search_string=request.args.get('search_string')

    lob=request.args.get('lob').lower()
    country=request.args.get('country')
    language=request.args.get('language','english').lower()
    doc_collection=request.args.get('doc_collection',None)
    def_sub_search=True if request.args.get('def_sub_search') == 'true' else False
    user_id=request.args.get('user_id','no_user_id')

    word_proximity_limit=int(request.args.get('word_proximity_limit',50))





    section=request.args.get('section',None)
    is_exclusion=True if section == 'exclusion' else False
    is_extension=True if section == 'extension' else False
    is_condition=True  if section == 'condition' else False
    is_definition=True  if section == 'definition' else False
    is_covered=True if  section == 'covered' else False


    sub_type=request.args.get('sub_type',None)
    is_endorsement=True  if sub_type == 'endorsement' else False
    is_basewording=True  if sub_type == 'base wording' else False
    is_expanded=True if request.args.get('is_expanded') == 'true' else False

    super_list=request.json['search_payload']


    category=request.args.get('category','default category')
    category_id=request.args.get('category_id','0')

    description=request.args.get('description')
    criteria_name=request.args.get('criteria_name','no name given')
    payload=api_payload["search_payload"]
    search_keywords=api_payload["search_keywords"]


    type_of_doc=request.args.get('type_of_doc','wordings').lower()
    search_index='{}-{}-{}'.format(lob,type_of_doc,version)
    save_type=request.args.get('save_type')




    parent_key=request.args.get('parent_key')

    query="SELECT * FROM c WHERE c.parent_key ='{}'".format(parent_key)

    items = list(saved_search_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    #deletion of standards when the coutry list is updated

    delet_countries=[i['id'] for i in items if i['country'] not in countries ]



    for id in delet_countries:
        query = "SELECT * FROM c WHERE c.id ='{}'".format(id)
        try:
            for item in saved_search_container.query_items(query,enable_cross_partition_query=True):
                saved_search_container.delete_item(item, partition_key=str(id))
            for item in assessment_report_container.query_items(query,enable_cross_partition_query=True):
                assessment_report_container.delete_item(item, partition_key=str(id))

        except Exception as err:
            return json.dumps({"status":"failed",'error':err})




    for country in countries:
        save_dict_found=next((item for item in items if item["country"] == country), None)
        if not save_dict_found:

            save_dict=dict()
            count=Ut.incremental_count(lob,country,saved_search_container)
            save_dict['id']=str(uuid.uuid4())
            save_dict['parent_key']=parent_key
            save_dict['inremental_count']=count

            save_dict['inremental_id']="{}-{}-{}-{}".format(country[:2].upper(),lob[:3].upper(),type_of_doc.upper(),count)


        else:
            save_dict=save_dict_found
        save_dict['search_string']=search_string


        save_dict['doc_collection']=doc_collection
        save_dict['lob']=lob
        save_dict['country']=country
        save_dict['countries_in_standard']=countries
        save_dict['language']=language
        save_dict['def_sub_search'] = def_sub_search
        save_dict['user_id']=user_id
        save_dict['word_proximity_limit']=word_proximity_limit

        save_dict['is_exclusion']=is_exclusion
        save_dict['is_extension']=is_extension
        save_dict['is_covered']=is_covered
        save_dict['is_condition']=is_condition
        save_dict['is_definition']=is_definition

        save_dict['is_endorsement']=is_endorsement
        save_dict['is_basewording']=is_basewording
        save_dict['is_expanded']=is_expanded
        save_dict['super_list']=super_list




        if section:
            save_dict['section']=section.split(',')
        else:
            save_dict['section']=[]
        if sub_type:
            save_dict['sub_type']=sub_type.split(',')
        else:
            save_dict['sub_type']=[]

        if doc_collection:
            save_dict['wording_type']=doc_collection.split(',')
        else:
            save_dict['wording_type']=[]




        save_dict['category']=category
        save_dict['category_id']=category_id

        #save_category
        save_dict['description']=description
        save_dict['criteria_name']=criteria_name
        save_dict['payload']=payload
        save_dict['search_keywords']=search_keywords


        save_dict['type_of_doc']=type_of_doc
        save_dict['search_index']=search_index
        save_dict['save_type']=save_type

        # read_item['country'] = 'Australia'
        if save_dict_found:
            response = saved_search_container.replace_item(item=save_dict, body=save_dict)
        else:

            saved_search_container.create_item(body=save_dict)


        # except Exception as err:
        #     return json.dumps({"status":"failed",'error':str(err)})

    return json.dumps({"status":"success","message":"updated the standards"})







@app.route('/view_saved_searches',methods = ['POST','GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def view_saved_searches():
    lob=request.args.get('lob')
    countries=request.args.get('country')
    country_query_item=countries.replace(",","','")
    user_id=request.args.get('user_id','no_user_id')
    language=request.args.get('language','english')
    query = "SELECT * FROM c WHERE c.lob ='{}' AND  c.country in ('{}') AND c.language='{}'".format(lob,country_query_item,language)
    items = list(saved_search_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    return json.dumps(items)







@app.route('/get_assessment_report',methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def get_assessment_report():
    lob=request.args.get('lob','casualty')
    countries=request.args.get('country')
    country_query_item=countries.replace(",","','")
    type_of_doc=request.args.get('type_of_doc')
    language=request.args.get('language')
    user_id=request.args.get('user_id','no_user_id')

    search_index='{}-{}-{}'.format(lob,type_of_doc,version)

    # response = assessment_report_container.read_item(item=id, partition_key=id)
    ############################################################



    query="SELECT * FROM c WHERE c.lob ='{}' AND  c.country in ('{}') AND c.type_of_doc='{}' AND c.language='{}' ".format(lob,country_query_item,type_of_doc,language)
    items = list(assessment_report_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))


    if not items:
        return json.dumps({})
    category_groups={}
    all_criterias=[]


    count=items[0]['count']
    timestamp=items[0]['timestamp']


    for item in items:
        if item['category_name'] not in category_groups:
            category_groups[item['category_name']]=[item]
        else:
            category_groups[item['category_name']].append(item)



    # return json.dumps(category_groups)

    save_dict_ret=[]
    for key,value in category_groups.items():


        value_sorted = sorted(value, key=key_func)

        all_parents=[]

        for parnt_key, child_standards in groupby(value_sorted, key_func):
            all_docs=[]
            found_count=0
            partial_found_count=0
            not_found_count=0
            parent_standard=[]



            for child in list(child_standards):
                if not parent_standard:
                    parent_standard=child
                all_docs.extend(child['docs'])
                found_count+=child['found_count']
                partial_found_count+=child['partial_found_count']
                not_found_count+=child['not_found_count']
            parent_standard['docs']=all_docs
            parent_standard['found_count']=found_count
            parent_standard['partial_found_count']=partial_found_count
            parent_standard['not_found_count']=not_found_count

            all_parents.append(parent_standard)


        save_dict_ret.append({'category_name':key,'category_id':int(value[0]['category_id']),'standards':all_parents})



    count_url=endpoint+"indexes/{}/docs/$count".format(search_index)+api_version
    count_response  = requests.get(count_url, headers=headers)
    latest_count=int(re.findall('\d+',count_response.text)[0])
    added_count=latest_count-count
    save_dict={
            'time': timestamp,
            'count': latest_count,
            'added_count':added_count,
            'categories':save_dict_ret
            }

    return json.dumps(save_dict)






@app.route('/saved_search_execution',methods = ['POST', 'GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def saved_search_execution():
    lob=request.args.get('lob','fire').lower()
    user_id=request.args.get('user_id','no_user_id')
    type_of_doc=request.args.get('type_of_doc','wordings').lower()


    search_index='{}-{}-{}'.format(lob,type_of_doc,version)

    count_url=endpoint+"indexes/{}/docs/$count".format(search_index)+api_version
    count_response  = requests.get(count_url, headers=headers)
    count=int(re.findall('\d+',count_response.text)[0])

    query = "SELECT * FROM c WHERE c.lob ='{}'".format(lob)
    items = list(saved_search_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))



    all_criterias=[]
    for search in items:
        # if search['parent_key']!='MD-AU-1':
        #     continue


        search_id=search['id']
        search_string=search['search_string']
        is_exclusion=search['is_exclusion']
        is_extension=search['is_extension']
        is_definition=search['is_definition']
        is_condition=search['is_condition']


        ##overriding as assessment is only for baswordings
        is_endorsement=False
        is_basewording=True



        super_list=search['super_list']

        is_covered=search["is_covered"]
        criteria_name=search['criteria_name']
        inremental_id=search['inremental_id']
        category=search['category']

        search['category_id']=int(search['category_id'])
        category_id=search['category_id']
        country=search['country']
        language=search['language']
        doc_collection=search['doc_collection']


        description=search['description']
        def_sub_search=search.get('def_sub_search',False)



        response_dict_total=perform_search(lob,search_index,"*",False,False,False,False,False,False,False,
            is_basewording,False,user_id,[],1000,3000,country,language,None)


        response_dict=perform_search(lob,search_index,search_string,doc_collection,is_exclusion,is_extension,
            is_covered,is_condition,is_definition,is_endorsement,is_basewording,def_sub_search,
            user_id,super_list,50,3000,country,language,None)

        response_dict_total_docs=copy.deepcopy(response_dict_total)

        # return(json.dumps(response_dict_total_docs))
        criteria_result=response_dict['results']

        total_docs_criteria=response_dict_total_docs['results']

        found_or_partial_docs=[result['doc_name'] for result in criteria_result]




        criteria={}

        criteria['standard_name']=criteria_name
        criteria['parent_key']=search['parent_key']
        criteria['category_name']=category
        criteria['category_id']=int(category_id)
        criteria['description']=description
        criteria['search_parameters']=search
        found_count=0
        partial_found_count=0
        not_found_count=0




        criteria_result_ids=[i['id'] for i in criteria_result]
        total_docs_criteria_ids=[i['id'] for i in total_docs_criteria]


        for result in total_docs_criteria:
            if result['id'] not in criteria_result_ids:
                result['score']=0
                criteria_result.append(result)



        criteria['docs']=[]

        criteria_result_sorted = sorted(criteria_result, key=key_func_result)



        all_documents=[]
        found_count=0
        partial_found_count=0
        not_found_count=0

        for doc_name, page_results in groupby(criteria_result_sorted, key_func_result):
            page_results_list=list(page_results)





            doc_result={}

            doc_scores=[i['score'] for i in page_results_list]



            max_score_doc=max(doc_scores)
            first_page=page_results_list[0]
            doc_result['language']=first_page['language']
            doc_result['doc_collection']=first_page['doc_collection']
            doc_result['doc_name']=first_page['doc_name']
            doc_result['page']=first_page['page']
            doc_result['country']=first_page['country']
            doc_result['score']=first_page['score']
            doc_result['lob']=first_page['lob']
            if max_score_doc >=80:
                doc_result['type']='found'
                found_count+=1
            elif 0< max_score_doc <80:
                doc_result['type']='partial_found'
                partial_found_count+=1
            else:
                doc_result['type']='not_found'
                not_found_count+=1








            criteria['docs'].append(doc_result)


        criteria['found_count']=found_count
        criteria['partial_found_count']=partial_found_count
        criteria['not_found_count']=not_found_count


        criteria['id']=search_id
        criteria['lob']=lob
        criteria['country']=country
        criteria['type_of_doc']=type_of_doc
        criteria['language']=language
        criteria['count']=count
        criteria['timestamp']=str(datetime.now()).split('.')[0]


        assessment_report_container.upsert_item(body=criteria)














        # return(json.dumps(grouped_results))





    return json.dumps({'status':'success','message':'executed assessment for {} {}'.format(lob,type_of_doc)})













@app.route('/search',methods = ['POST','GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def search():
    search_string=request.args.get('search_string')
    section=request.args.get('section')
    is_exclusion=True if section == 'exclusion' else False
    is_condition=True  if section == 'condition' else False
    is_definition=True  if section == 'definition' else False
    is_extension=True  if section == 'extension' else False
    is_covered=True if  section == 'covered' else False



    doc_collection=request.args.get('doc_collection')
    doc_name=request.args.get('doc_name',None)
    country=request.args.get('country')
    sub_type=request.args.get('sub_type')

    is_endorsement=True  if sub_type == 'endorsement' else False
    is_basewording=True  if sub_type == 'base wording' else False
    is_expanded=True if request.args.get('is_expanded') == 'true' else False

    def_sub_search=True if request.args.get('def_sub_search') == 'true' else False
    lob=request.args.get('lob','casualty').lower()
    language=request.args.get('language','english').lower()
    user_id=request.args.get('user_id','no_user_id')
    type_of_doc=request.args.get('type_of_doc','wordings').lower()
    if language =='spanish':
        search_index='{}-wordings-spanish'.format(lob)
    else:
        search_index='{}-{}-{}'.format(lob,type_of_doc,version)
    word_proximity_limit=int(request.args.get('word_proximity_limit',50))









    if search_string=='*':
        super_list=[]
    else:
        super_list=request.json

    response_dict=perform_search(lob,search_index,search_string,doc_collection,
        is_exclusion,is_extension,is_covered,is_condition,is_definition,is_endorsement,
        is_basewording,def_sub_search,user_id,super_list,word_proximity_limit,3000,
        country,language,doc_name)

    return json.dumps(response_dict)




















@app.route('/retrieve_result',methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def retrieve_result():
    if request.method == 'GET':

        result_id=request.args.get('result_id')
        user_id=request.args.get('user_id','no_user_id')
        lob=request.args.get('lob','fire')
        country=request.args.get('country','all')
        word_proximity_limit=int(request.args.get('word_proximity_limit',50))
        search_result=copy.deepcopy(search_result_global[user_id])
        result=search_result[result_id]
        page_num=int(result['page'])
        highlight_words=result['highlight_words']
        result['key_phrases']=result['bold_phrases']


        if lob=='fire' and country =='all':
            encoded_string=Ut.pdf_highlighter(result['doc_name'],highlight_words,'storage',user_id)
            doc_type='pdf'
        else:
            encoded_string=Ut.pdf_highlighter(result['doc_name'],highlight_words,'blobs',user_id)
            doc_type='docx'


        return json.dumps({"page":result['page'],"file_string":str(encoded_string),"features":result,"doc_type":doc_type})

@app.route('/retrieve_result_save_search',methods = ['POST'])
@auth.login_required
@cross_origin(supports_credentials=True)
def retrieve_result_save_search():
    if request.method == 'POST':

        user_id=request.args.get('user_id','no_user_id')
        lob=request.args.get('lob','casualty')
        country=request.args.get('country','australia')
        if country=='australia':
            country_code='AU'
        elif country=='new zealand':
            country_code='NZ'
        elif country=='singapore':
            country_code='SG'
        elif country=='uk':
            country_code='UK'
        elif country=='chile':
            country_code='CH'
        elif country=='argentina':
            country_code='AR'
        elif country=='colombia':
            country_code='CO'

        req_body=request.json
        highlight_words=req_body['highlight_words']
        highlight_words_def=req_body.get('highlight_words_def',[])
        docname=request.args.get('doc_name')
        type_of_doc=request.args.get('type_of_doc')
        doc_collection=request.args.get("doc_collection")
        page_num=request.args.get('pagenum')
        highlight_path="{}-{}".format(country_code,lob.title())
        phrase_list = req_body.get("phrase_list",[])
        #print("phrase_list",phrase_list)
        encoded_string=Ut.pdf_highlighter(docname,highlight_words,highlight_words_def,highlight_path,user_id,country_code,doc_collection,phrase_list,page_num)



        return json.dumps({"file_string":str(encoded_string)})






@app.route('/click_tag',methods = ['POST', 'GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def click_tag():
    key_phrase=request.args.get('key_phrase')
    lob=request.args.get('lob','fire')
    country=request.args.get('country','all')
    doc_name=request.args.get('doc_name')
    user_id=request.args.get('user_id','no_user_id')
    encoded_string=Ut.click_pdf_highlighter(doc_name,key_phrase,user_id)

    return json.dumps({"file_string":str(encoded_string)})



@app.route('/get_categories',methods = ['GET'])
@auth.login_required
@cross_origin(supports_credentials=True)
def get_categories():
    categories=[{"name":'General','id':1},
                    {"name":'Coverage Grant','id':2},
                    {"name":'Main definitions','id':3},
                    {"name":'Exclusions','id':4},
                    {"name":'Cover Extensions','id':5},
                    {"name":'Policy Conditions','id':6},
                    {"name":'Regulatory Requirements','id':7}
                    ]
    return json.dumps(categories)


def delete_contents(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))




# def get_response(x):
#     time.sleep(3)
#     for i in range(x*x):
#         out=i*x
#     return out
# @app.route('/callseq/')
# def health_check_seq():

#     resp_pool = []
#     for i in range(1000,1005):
#         resp_pool.append(get_response(i))

#     return str([i for i in resp_pool ])

@app.route('/call/')
def health_check():

    
     

    return json.dumps({"cpu_count":multiprocessing.cpu_count()})

if __name__ == '__main__':
    print("hello")
    try:
        _pool = Pool(processes=5)
        Swagger(app, template_file='swagger.yaml')

        # app.config['PROFILE'] = True
        # app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30])
        app.run(debug=false,host='0.0.0.0', port=80)
    except KeyboardInterrupt:
        _pool.close()
        _pool.join()
