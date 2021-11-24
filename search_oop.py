from collections import Counter
from cognitive_search import Search
from operator import itemgetter
from itertools import groupby
from itertools import chain
import re
import json
import requests
from fuzzywuzzy import fuzz
from load import init
from datetime import datetime
from utils import Utils as Ut
from nltk.corpus import stopwords
stop_words_english = set(stopwords.words('english'))
stop_words_spanish = set(stopwords.words('spanish'))
stop_words = {'english': stop_words_english, 'spanish': stop_words_spanish}
support_syntax_re = re.compile(r'\(|\)|/|"|\.|;|:|\[|\]')


env = init()
endpoint = env['azure_search_endpoint']
version = env['version']
api_version = env['api_version']
headers = {'Content-Type': 'application/json',
           'api-key': env['api_key']}

highlight_re = re.compile("<em>(.*?)</em>")
required_keys = ['text', "matchType", "section", "relevance", "language", "country", "page", "lob",
                 "sublob", "phraseList", "highlightWordsDef", "boldPhrases", "highlightWords",
                 "definitionsInPage", "score", "policyNo", "expiryDate", "effectiveDate", "documentName", "documentType","section_bow"]


def perform_search_tt(_pool, searchValues, searchString, LOB, country,
                      documentType, language, effectiveDateFrom, EffectiveDateTo, SLOB,
                      sections, substituteDefinition, expandedSearch):

    if sections == ['all']:
        sections = ['exclusion', 'definition', 'condition',
                   'extension', 'covered sections', 'miscellaneous']



    results_from_sections = search_on_section(_pool,searchValues, searchString, LOB, country, documentType,
              language, effectiveDateFrom, EffectiveDateTo, SLOB, sections,
              substituteDefinition, expandedSearch, 1000)

    aggregated_result = Search.group_result(results_from_sections)

    return aggregated_result





def search_on_section(_pool,request_payload, search_string, lob, country, document_type, language, effectiveDateFrom, EffectiveDateTo, s_lob, sections, def_sub_search, expanded_search, top_count):




    # word_proximity_limit = 50
    country = 'uk'
    language = "english"

    # print(def_sub_search)
    search_obj= Search(sections,lob,s_lob,def_sub_search,language,country,top_count,document_type)

    if search_string == '*':
        super_list = []
    else:
        super_list = request_payload



    # spell check for single phrase search

    super_list, clauses, clause_operator = Ut.payload_to_super_list_n_clauses(super_list)

    search_obj.assignment(search_string)

    # Work around untill the problem is addressed from the fron<t end
    search_string = search_string.replace('&', '').replace('\n', '').replace(':', '')

    all_results = []
    clause_result = []
    prev = False
    prev_list = []

    for clause_string in clauses:
        clause_string_org = clause_string

        # Work around untill the problem is addresed from the front end
        clause_string = clause_string.replace(
            '&', '').replace('\n', '').replace(':', '')
        search_string = requests.utils.unquote(search_string)
        # clause_string = clause_string.replace(u"â€™",u"’")
        search_string = search_string.replace(clause_string, '')
        clause_string = support_syntax_re.sub(' ', clause_string)
        clause_string = clause_string.replace('"', '').replace(
            '(', '').replace(')', '').replace(';', ' ').replace(':', ' ')
        search_words = clause_string.split()

        search_words = [w for w in search_words if w.lower()
                        not in stop_words[language]]
        search_word_len = len(search_words)

        clause_string = (' ').join(search_words)


        if prev:
            prev_list = out_all

        out_all, searchFields = search_obj.dynamic_query( clause_string)
        # for page in out_all:
        #     print(page['@search.score'] ,search_word_len*len(page["@search.highlights"])*1.05,page['page'])
        # return out_all
        prev = True

        if not clause_operator:
            for page in out_all:
                # print(page['page'],'...',page['@search.score'], (search_word_len*len(page["@search.highlights"]))*0.95)

                if page['@search.score'] > (search_word_len*len(page["@search.highlights"])*0.95):
                    all_results.append(
                            (page, True, clause_string, clause_string_org))


        else:

            if prev_list:

                p = out_all.copy()
                out_all = []
                for res in prev_list:
                    intersection = False
                    intersection_list=[]

                    for key, value in res.items():
                        for i in p:

                            if i["doc_name"] == value["doc_name"] and i['@search.score'] > (search_word_len*len(value["@search.highlights"])):


                                intersection = True
                                intersection_list.append(i)


                        if intersection:
                            break

                    if len(intersection_list)>=1:
                        new_dic = res
                        for result in intersection_list:
                            new_dic[clause_string_org+str("_"+result["page"])] = result
                        out_all.append(new_dic)
                clause_result = out_all

            else:


                out_all = [{clause_string_org+str("_"+res["page"]): res} for res in out_all if res['@search.score'] > (search_word_len*len(res["@search.highlights"]))]
                clause_result = out_all

 

    search_string = re.sub('(\((?:\sOR\s|\sAND\s)*\))', '###', search_string)

    rightRE = re.compile('(AND|OR)\s+###')
    leftRE = re.compile('###\s+(AND|OR)')

    search_string = rightRE.sub('', search_string)

    search_string = leftRE.sub('', search_string)

    search_string = re.sub(re.escape('###'), '', search_string).strip()
    keyword_results = []


    # condition for keyword search, making sure that null is not searched after removing the clauses from Search string
    if (len(search_string) > 4 or search_string == '*') and not clauses:

        try:
            out_all, searchFields = search_obj.dynamic_query(search_string,keyword=True)
            keyword_results = [(item, False, '', '') for item in out_all]

        except Exception as err:
            print(err)


    if clause_operator:
        for res in clause_result:
            for key, value in res.items():
                if "_" in key:
                    key=key.split("_")[0]
                if keyword_results:

                    for j in out_all:
                        if value["doc_name"] == j["doc_name"] and value["page"] == j["page"]:

                            all_results.append((value, True, key, key))
                            all_results.append((j, False, '', ''))
                            continue
                else:
                    all_results.append((value, True, key, key))

    else:
        if keyword_results:
            all_results.extend(keyword_results)

    if not all_results:
        return{"results": [], 'proximity_flag': False, 'key_phrases_aggregate': [], 'min_proximity': 0, 'max_proximity': 0}


    # for page in all_results:
    #     print(page[0]['page'])
    flattened_reults_sections=search_obj.flatten_for_sections(all_results)
    # return flattened_reults_sections








    flattened_reults_sections=[(i,super_list) for i in flattened_reults_sections]

    resp_pool = _pool.map(search_obj.validate_result,flattened_reults_sections)


    matches_list,is_clause=[i[0] for i in resp_pool if i[0]],[i[1] for i in resp_pool if i[1]]


    matches_list=search_obj.assign_score(matches_list,is_clause,search_string)
    


    key_phrases_bold_aggregate = []
    for match in matches_list:
        if match['score'] >= 80:
            key_phrases_bold_aggregate.extend(match['boldPhrases'])

    # When clause and keyword search has reults from the same page we pick the one that has highes score so that there will be only one result from one pagenum
    pages_n_scores = {}
    final_matches_list = []
    if search_string == '*':
        final_matches_list = matches_list
    elif clause_operator and len(clauses) >= 2:

        key_counts = Counter(d['documentName'] for d in matches_list)

        duplicateValues = dict()
        for res in matches_list:
            if key_counts[res['documentName']] >= len(clauses):
                key = res['documentName']+str(res['page'])+res['section']
                if key not in duplicateValues:
                    duplicateValues[key] = [res]
                else:
                    prev = duplicateValues[key]
                    if res not in prev:
                        prev.append(res)

                        duplicateValues[key] = prev
        for key, value in duplicateValues.items():

            text, clause_score, score = "", 0.0, 0.0
            highlight_words = []

            new_dic = {k: v for k, v in value[0].items()}

            for page in value:
                # print(page.keys())
                text += page["text"]
                clause_score += page["clause_score"]



                highlight_words.extend(page.get("highlightWords",""))
            new_dic["text"] = text


            new_dic["clause_score"] = clause_score/len(value)
            new_dic["score"] =new_dic["clause_score"]
            new_dic["matchType"], new_dic["relevance"] = Ut.search_relevance_clause(new_dic["clause_score"])
            new_dic["highlightWords"] = highlight_words
            final_matches_list.append(new_dic)

    else:

        for match in matches_list:
            page = match['page']
            score = match['score']
            doc_name = match['documentName']

            if (doc_name, page,match['section']) not in pages_n_scores:
                if score != 0:
                    pages_n_scores[(doc_name, page,match['section'])] = score
                    final_matches_list.append(match)
            else:
                if score != 0 and score > pages_n_scores[(doc_name, page,match['section'])]:
                    pages_n_scores[(doc_name, page,match['section'])] = score
                    final_matches_list.append(match)
                else:
                    pass

    final_matches_list_clean = []
    for match in final_matches_list:

        clean_match = {key: match[key] for key in required_keys}
        final_matches_list_clean.append(clean_match)


    response_dict = {"results": final_matches_list_clean}
    proximity_scores = [match['proximity']
                        for match in final_matches_list if 'proximity' in match]
    if proximity_scores:
        response_dict['min_proximity'] = min(proximity_scores)
        response_dict['max_proximity'] = max(proximity_scores)
        if list(set(proximity_scores))[0] != 0:
            response_dict['proximity_flag'] = True
        else:
            response_dict['proximity_flag'] = False

    else:
        response_dict['proximity_flag'] = False

    response_dict['key_phrases_aggregate'] = Ut.kephrase_process(
        key_phrases_bold_aggregate)

    return response_dict
