from load import init
from flask import request
import requests
import copy
from utils import Utils as Ut
import re
from itertools import groupby
import json

from nltk.corpus import stopwords
stop_words_english = set(stopwords.words('english'))
stop_words_spanish = set(stopwords.words('spanish'))
stop_words = {'english': stop_words_english, 'spanish': stop_words_spanish}
env = init()

endpoint = env['azure_search_endpoint']
version = env['version']
api_version = env['api_version']
headers = {'Content-Type': 'application/json',
           'api-key': env['api_key']}
search_index = 'tt-documents-{}'.format(version)

highlight_re = re.compile("<em>(.*?)</em>")
excl_bow=['exclud','not cover','except','does not mean','not includ','exclusion']
cond_bow=['condition']
ext_bow=['extension']
bag_of_words={"excl_bow":excl_bow,"cond_bow":cond_bow,"ext_bow":ext_bow}


class Search:

    def __init__(self,sections,lob,sub_lob,def_sub_search,language,country,top_count,document_type):
        self.sections =sections
        self.word_proximity_limit = 50
        self.lob= lob
        self.sub_lob=sub_lob
        self.def_sub_search= def_sub_search
        self.country= country
        self.language=language
        self.top_count=top_count
        self.document_type=document_type

    def assignment(self,search_string):
        self.is_exclusion = True if 'exclusion' in self.sections else False
        self.is_condition = True if 'condition' in self.sections else False
        self.is_definition = True if 'definition' in self.sections else False
        self.is_extension = True if 'extension' in self.sections else False
        self.is_covered = True if 'covered sections' in self.sections else False
        self.is_miscellaneous = True if 'miscellaneous' in self.sections else False


        # checking if AND operator is present in the search string, if yes, the scoring algorithm should be proximity based

        if 'AND' in search_string:
            self.and_operator_boolean = True
        else:
            self.and_operator_boolean = False


    def get_result(self,search_query, search_string):
        url = endpoint + 'indexes/{}/docs'.format(search_index) + api_version + search_query
        response = requests.get(url, headers=headers, json=search_string)

        out_all = []

        out = response.json()

        if 'value' in out:
            out_all.extend(out['value'])

        score_break = 1
        while '@odata.nextLink' in out and score_break >1:

            url = out['@odata.nextLink']
            response = requests.get(url, headers=headers, json=search_string)
            print('Iteration',url)
            out = response.json()
            score_break = out['value'][0]['@search.score']
            out_all.extend(out['value'])
        return out_all


    def dynamic_query(self, search_string,keyword=False):


        searchFields=['plain_text']
        highlight=['plain_text']

        if self.is_definition:
            searchFields.append('definition_text')
            highlight.append('definition_text')
        if self.def_sub_search:
            searchFields.append('text')
            highlight.append('text')
        if self.is_exclusion:
            searchFields.append('excl_text')
            highlight.append('excl_text')       
        if self.is_extension:
            searchFields.append('ext_text')
            highlight.append('ext_text')
        if self.is_condition:
            searchFields.append('cond_text')
            highlight.append('cond_text')
        if self.is_covered:
            searchFields.append('covered_text')
            highlight.append('covered_text')



        searchFields=(',').join(searchFields)
        highlight=(',').join(highlight)

        if keyword:
            self.top_count=500



        search_query = "&search={}&$count=true&highlight={}&queryType=full&searchFields={}&$top={}".format(
            search_string, highlight, searchFields, self.top_count)
        filters = "&$filter="
        # filters += "doc_name eq 'REDACTED_E402ECEC-A91E-4531-A8C2-88F782CB548E'  and "
        if self.document_type == ['endorsements']:
            filters += "document_type eq 'Endorsements' or endorsement_flag and "
        elif self.document_type == ["policies"]:
            filters += "document_type eq 'Policy Documents' and not endorsement_flag and "
        else:
            pass


        if not self.is_miscellaneous:
    
            if self.is_exclusion:
                filters += "IsExclusion and "
            if self.is_extension:
                filters += "ext_flag and "
            if self.is_covered:
                filters += "covered_flag and "

            if self.is_definition:
                filters += "definition_flag and "
            if self.is_condition:
                filters += "cond_flag and "
        

        filters += "search.in(country,'{}', ',')".format(self.country)
        filters += " and language eq '{}'".format(self.language)
        search_query += filters
        result = self.get_result(search_query, search_string)
        return result, searchFields


    def flatten_for_sections(self,all_results):
        flat_results=[]

        for result_org,i,j,k in all_results:

            if self.is_exclusion and result_org['IsExclusion'] and 'plain_text' in result_org['@search.highlights']:
                result=copy.deepcopy(result_org)


                result['result_section']='exclusion'

                flat_results.append((result,i,j,k))
            if self.is_condition and result_org['cond_flag'] and 'cond_text' in result_org['@search.highlights']:
                result=copy.deepcopy(result_org)


                result['result_section']='condition'
                flat_results.append((result,i,j,k))

            if self.is_extension and result_org['ext_flag'] and 'plain_text' in result_org['@search.highlights']:
                result=copy.deepcopy(result_org)
                result['result_section']='extension'
                flat_results.append((result,i,j,k))

            if self.is_covered and result_org['covered_flag'] and 'covered_text' in result_org['@search.highlights']:
                result=copy.deepcopy(result_org)
                result['result_section']='covered sections'
                flat_results.append((result,i,j,k))

            if self.is_definition and result_org['definition_flag'] and 'definition_text' in result_org['@search.highlights'] :
                result=copy.deepcopy(result_org)
                result['result_section']='definition'
                flat_results.append((result,i,j,k))

            if self.is_miscellaneous and  not any([result_org['definition_flag'],result_org['IsExclusion'],result_org['covered_flag'],result_org['ext_flag'],result_org['cond_flag']]):
                result=copy.deepcopy(result_org)
                result['result_section']='miscellaneous'
                flat_results.append((result,i,j,k))



        return flat_results






    def validate_result(self,all_results_n_super_list):




        all_results,super_list=all_results_n_super_list
        flat_super_list = [item for sublist in super_list for item in sublist]

        flat_super_list = [item for sublist in flat_super_list for item in sublist]
        


        matches_list = []
        # search_scores = []
        # proximity_scores = []
        search_result = dict()

        page, is_clause, clause_string, clause_string_org = all_results
            # search_scores.append(page['@search.score'])
        content_exl_head = False
        content_cond_head = False
        content_ext_head = False
        section_bow = None
        proximity_lookup_text=""
        if '@search.highlights' in page:
            if is_clause:

                # try:
                if page['result_section'] =='exclusion' and 'excl_text' in page['@search.highlights']:
                        clause_text=page['excl_text']
                elif page['result_section'] =='condition' and 'cond_text' in page['@search.highlights']:
                        clause_text=page['cond_text']
                elif page['result_section'] =='definition' and 'definition_text' in page['@search.highlights']:
                        clause_text=page['definition_text']
                elif page['result_section'] =='covered sections' and 'covered_text' in page['@search.highlights']:
                        clause_text=page['covered_text']

                elif page['result_section'] =='extension' and 'ext_text' in page['@search.highlights']:
                        clause_text=page['ext_text']
                else:
                    return None,None
                    
                   

                    # clause_text

                # print(page['policy_id'],page['page'],page['result_section'])

                match_text, clause_score = Ut.clause_search(clause_text, clause_string_org)

                # print(clause_string_org,'.....',match_text,page['page'],'....',clause_score,'\n\n\n')
                if page['result_section'] =='definition' and 'definition_text' in page['@search.highlights'] and not match_text:
                    match_text_plain, clause_score = Ut.clause_search(page['plain_text'], clause_string_org)
                    if clause_score > 85 :
                        match_text=match_text_plain

                

                page['clause_score'] = clause_score
                if not match_text:
                    return None,None


                # except Exception as err:

                #     print("Exception in clause search!!!!!!!!", str(err))
                #     return None,None
                    # match_text=('....').join(page['@search.highlights'][highlight])
            else:
                match_text=''
                if self.is_definition and page['result_section']=='definition':

                    match_text = ('....').join(
                        page['@search.highlights']['definition_text'])

                    proximity_lookup_text=page['definition_text']

                

                if self.is_exclusion and page['result_section']=='exclusion':
                    try:
                        
                        match_text = ('....').join(
                            page['@search.highlights']['excl_text'])
                        proximity_lookup_text=page['excl_text']
                        content_exl_head = True
                    except:



                        head_check_bow=Ut.check_heading_bow(page['plain_text'],flat_super_list,page['header_pos'],page['excl_pos'])
                        if not head_check_bow:
                            return None,None
                        match_text = ('....').join(
                            page['@search.highlights']['plain_text'])
                        proximity_lookup_text=page['plain_text']
                if self.is_condition and page['result_section']=='condition':
                    try:
                        
                        match_text = ('....').join(
                            page['@search.highlights']['cond_text'])
                        proximity_lookup_text=page['cond_text']
                        content_cond_head = True
                    except:
                        match_text = ('....').join(
                            page['@search.highlights']['plain_text'])
                        proximity_lookup_text=page['plain_text']
                if self.is_covered and page['result_section']=='covered sections':

                    
                    match_text = ('....').join(
                        page['@search.highlights']['covered_text'])
                    proximity_lookup_text=page['covered_text']
                    content_ins_ag_head = True


                if self.is_extension and page['result_section']=='extension':
                    try:

                        match_text = ('....').join(
                            page['@search.highlights']['ext_text'])
                        proximity_lookup_text=page['ext_text']
                        content_ext_head = True
                        # print(content_ext_head,'EXT HEAD')
                    except Exception as err:

                        # print('\n\n...',page['page'])
                        head_check_bow=Ut.check_heading_bow(page['plain_text'],flat_super_list,page['header_pos'],page['ext_pos'])
                        if not head_check_bow:
                            return None,None
                        match_text = ('....').join(page['@search.highlights']['plain_text'])
                        proximity_lookup_text=page['plain_text']

                if self.def_sub_search:
                    match_text = ('....').join(page['@search.highlights']['text'])
                    proximity_lookup_text=page['text']


        else:
            match_text = ''
        highlights = page["@search.highlights"]
        if highlights.get("cond_text") is  None and page['result_section']=='condition':
            section_bow = "cond_bow"
        if highlights.get("ext_text") is  None and page['result_section']=='extension':
            section_bow = "ext_bow"
        if highlights.get("excl_text") is  None and page['result_section']=='exclusion':
            section_bow = "excl_bow"




        highlight_words = list(set(highlight_re.findall(match_text)))
        highlight_words_stopword = [word for word in highlight_words if word.lower() in stop_words[self.language]]
        highlight_words = [word for word in highlight_words if word.lower() not in stop_words[self.language]]
        highlight_words_user = highlight_words
        plain_text=page['plain_text']
        proximity_lookup_text=plain_text
        definition_text=page['definition_text']


        highlight_words_def = []


        if not is_clause:

            plain_text = plain_text.replace(':', '')

            high_light_words = Ut.get_highlightWords(plain_text, flat_super_list)
            if high_light_words:
                high_light_words.sort(key=len, reverse=True)
            else:
                return None,None
            for word in high_light_words:
                r = re.compile(r"\s+", re.MULTILINE)
                plain_text = r.sub(" ", plain_text)
                phrase_re = re.compile(re.escape(' '+word), re.I)
                plain_text = phrase_re.sub('<em>'+word+'</em>', plain_text)
                # When the word comes inside double quotes
                plain_text = plain_text.replace(
                    "\""+word, "\""+'<em>'+word+'</em>')

            if self.def_sub_search:
                for phrase in highlight_words_def:
                    phrase_re = re.compile(re.escape(phrase), re.I)
                    plain_text = phrase_re.sub(
                        '<ed>'+phrase+'</ed>', plain_text)


        # From bold phrases

        key_phrases_bold = page['bold_phrases']
        key_phrases_bold = [i for i in key_phrases_bold if len(i) > 3]
        key_phrases_bold = [i for i in key_phrases_bold if Ut.get_phrase_proximity(
            highlight_words, i, page['text'], super_list) <= self.word_proximity_limit]

        matches = {}
        if is_clause:
            matches['text'] = match_text
            matches['clause_score'] = clause_score
            matches["matchType"], matches["relevance"] = Ut.search_relevance_clause(
                clause_score)

        else:
            matches['text'] = plain_text
        matches["section_bow"] = section_bow
        matches['azure_score'] = page['@search.score']
        matches['language'] = page['languageCode']
        matches['page'] = int(page['page'])
        matches['documentName'] = page['doc_name']
        matches['country'] = page['country']
        matches['policyNo'] = page['policy_id']
        matches['sublob'] = self.sub_lob
        matches['expiryDate'] = page['effective_till'][0:10]
        matches['effectiveDate'] = page['effective_from'][0:10]
        matches['section'] = page['result_section']


        header_pos = page['header_pos']
        # print(header_pos) phraseList highlightWordsDef
        matches['lob'] = self.lob
        matches["phraseList"] = flat_super_list

        matches['is_clause'] = is_clause

        matches['id'] = page['id']
        def_search_list = []
        if self.def_sub_search:
            for defs in page['definitions']:
                for word in highlight_words_user:
                    if word in defs['text']:
                        word_re = re.compile(re.escape(word), re.I)
                        defs['text'] = word_re.sub(
                            '<em>'+word+'</em>', defs['text'])
                        if defs not in def_search_list:
                            def_search_list.append(defs)
            matches['highlightWordsDef'] = highlight_words_def
        else:
            matches['highlightWordsDef'] = []

        matches['definitions'] = def_search_list
        matches['boldPhrases'] = key_phrases_bold
        matches['highlightWords'] = highlight_words_user

        # return(json.dumps(page))
        matches['definitionsInPage'] = page['definitions_in_page']

        matches['definition_text'] = definition_text
        if page['document_type']=="Policy Docs":
            page['document_type']="Policy Documents"
        matches['documentType'] =page['document_type']
        header_flag = False
        # print('\n\n\n...',page['page'],header_pos)
        if not is_clause:
            proximity_dict = {}
            for master_list in super_list:
                if len(master_list) > 1 and self.and_operator_boolean:
                    proximity_in_word_count, cropped_text, header_flag = Ut.proximity_header_excluded(
                        proximity_lookup_text, master_list, header_pos)
                    
                    proximity_dict[cropped_text] = proximity_in_word_count
                elif len(master_list) > 1 and not self.and_operator_boolean:
                    proximity_in_word_count, cropped_text = Ut.proximity(
                        proximity_lookup_text, master_list)
                    proximity_dict[cropped_text] = proximity_in_word_count
                elif len(master_list) == 1:
                    proximity_in_word_count, cropped_text = Ut.single_level_proximity(
                        proximity_lookup_text, master_list)
                    proximity_dict[cropped_text] = proximity_in_word_count
            if proximity_dict:
                cropped_text = min(proximity_dict, key=proximity_dict.get)
                proximity_in_words = proximity_dict[cropped_text]
            else:

                cropped_text = 'Not applicable'
                proximity_in_words = 0


            matches['proximity'] = proximity_in_words
            # print(proximity_in_word_count,header_flag)
            matches["matchType"], matches["relevance"] = Ut.search_relevance_keyword(
                proximity_in_words, sub_heading=header_flag)
            # proximity_scores.append(proximity_in_words)
            matches['proximity_crop'] = cropped_text

            if cropped_text == 'Not applicable':
                matches['custom_score'] = 100
            elif cropped_text == '':
                matches['custom_score'] = 0
                return None,None
            elif proximity_in_words > 0:
                total_words = len(matches['text'].split())
                matches['custom_score'] = (
                    1-(proximity_in_words/total_words))*100


            if self.is_exclusion and matches['section']=='exclusion':
                if not content_exl_head and not Ut.section_confidence_bow(highlight_words_user,excl_bow,plain_text):
                    return None,None

            if self.is_condition and  matches['section']=='condition':
                if not content_cond_head and not Ut.section_confidence_bow(highlight_words_user,cond_bow,plain_text):
                    return None,None
            if self.is_extension and matches['section']=='extension':
                if not content_ext_head and not Ut.section_confidence_bow(highlight_words_user,ext_bow,plain_text):
                    return None,None



        fuzz_ratio = Ut.find_def_in_keys(
            highlight_words_user, page['definitions_in_page'])[1]
        if Ut.find_def_in_keys(highlight_words_user, page['definitions_in_page'])[0]:
            matches['definition_confidence'] = round(80+(fuzz_ratio/5))

        else:
            try:
                matches['definition_confidence'] = round(
                    min(80, matches['custom_score']))
            except:
                matches['definition_confidence'] = 80






        page['highlightWords'] = highlight_words_user
        page['highlightWordsDef'] = highlight_words_def
        page['bold_phrases'] = key_phrases_bold



        return matches,is_clause

    def assign_score(self,matches_list,is_clause,search_string):

        if len(matches_list) == 1 and is_clause:
            matches_list[0]['score'] = matches_list[0]['clause_score']

        elif len(matches_list) == 1 and not self.is_definition:
            matches_list[0]['score'] = matches_list[0]['custom_score']
        elif len(matches_list) == 1 and self.is_definition:
            matches_list[0]['score'] = matches_list[0]['definition_confidence']
        else:

            for match in matches_list:
                try:

                    if match['is_clause'] and not self.is_definition:
                        match['score'] = match['clause_score']

                    elif self.is_definition and not self.and_operator_boolean:
                        match['score'] = match['definition_confidence']



                    elif search_string == '*':
                        match['score'] = 0

                    elif 'custom_score' in match:
                        match['score'] = match['custom_score']
                    else:
                        match['score'] = match['azure_score']

                except Exception as err:
                    print('Exception while reassigning score:{}'.format(err))
                    match['score'] = 0
        return matches_list

    @staticmethod
    def group_result(results_from_sections):
        flattened_result =results_from_sections['results']

        def key_func_doc(k):
            return k['documentName']

        def key_func_match_type(k):
            return k['matchType']

        def key_func_section(k):
            return k['section']

        sorted_result_match_type = sorted(
            flattened_result, key=key_func_match_type)
        grouped_by_match_type = {}
        for key, value in groupby(sorted_result_match_type, key_func_match_type):
            matches = list(value)
            sorted_matches_section = sorted(matches, key=key_func_section)
            grouped_by_section = {}
            for k, v in groupby(sorted_matches_section, key_func_section):
                grouped_by_section[k] = len(list(v))
            grouped_by_match_type[key] = grouped_by_section

        total_count = 0
        for match_type, value in grouped_by_match_type.items():
            match_type_total = 0
            for section, count in value.items():
                if section != 'miscellaneous':
                    total_count += count
                    match_type_total += count
            value['total'] = match_type_total

        grouped_by_match_type['total'] = total_count

        sorted_result_docname = sorted(flattened_result, key=key_func_doc)
        grouped_by_doc = []
        for key, value in groupby(sorted_result_docname, key_func_doc):
            doc_dict = {'documentName': key}
            matches = list(value)

            high_flag, med_flag, low_flag, full_match = False, False, False, False
            doc_dict = {'documentName': key}
            doc_dict['matches'] = matches
            for match in matches:
                if match['matchType'] == 'full':
                    full_match = True
                    continue
                else:
                    if match['relevance'] == 'High':
                        high_flag = True
                        continue
                    if match['relevance'] == 'Medium':
                        med_flag = True
                    if match['relevance'] == 'Low':
                        low_flag = True

            if full_match:
                doc_dict['matchType'] = 'full'
                doc_dict['relevance'] = 'High'
            else:
                if high_flag:
                    doc_dict['matchType'] = 'partial'
                    doc_dict['relevance'] = 'High'
                elif med_flag and not high_flag:
                    doc_dict['matchType'] = 'partial'
                    doc_dict['relevance'] = 'Medium'
                else:
                    doc_dict['matchType'] = 'partial'
                    doc_dict['relevance'] = 'Low'
            doc_dict['matchCount'] = len(matches)
            doc_dict['policyDetails'] = {
                "policyNo": matches[0]['policyNo'],
                "expiryDate": matches[0]['expiryDate'],
                "effectiveDate": matches[0]['effectiveDate']
            }
            grouped_by_doc.append(doc_dict)

        aggregated_result = {
            "abstract": grouped_by_match_type,
            "results": grouped_by_doc}

        return aggregated_result





"""
https://pip-search-poc.search.windows.net/indexes/tt-documents-dev/docs?api-version=2019-05-06&search=Tenants Neighbours Liability insurance Section extended indemnify Insured legal liability imposed Insured Napoleonic similar civil commercial code force country respect loss destruction damage property- occupied Insured b neighbours co-tenants caused fire explosion spreading Premises neighbours co-tenants c sub-tenants result constructional defects lack maintenance Insured landlord&$count=true&highlight=plain_text&queryType=full
&searchFields=plain_text&$top=500&$filter=search.in(country,'uk', ',') and language eq 'english'
https://pip-search-poc.search.windows.net/indexes/tt-documents-dev/docs?api-version=2019-05-06&search=(Tenants%20and%20Neighbours%20Liability%20The%20insurance%20by%20Section%20is%20extended%20to%20indemnify%20the%20Insured%20against%20any%20legal%20liability%20imposed%20on%20the%20Insured%20in%20any%20by%20the%20Napoleonic%20(or%20similar%20civil%20or%20commercial%20code)%20in%20force%20in%20country%20in%20respect%20of%20loss%20or%20destruction%20of%20or%20damage%20to%20property%3A-%20a%20occupied%20by%20the%20Insured%20as%20%20b%20of%20neighbours%20and%20co-tenants%20caused%20by%20fire%20(or%20explosion)%20spreading%20from%20the%20Premises%20to%20the%20%20of%20such%20neighbours%20and%20co-tenants%20c%20of%20or%20sub-tenants%20of%20the%20as%20a%20result%20of%20constructional%20defects%20or%20lack%20of%20maintenance%20by%20the%20Insured%20as%20landlord.)&$count=true&highlight=plain_text&queryType=full&searchFields=plain_text&$top=500&$filter=search.in(country,'uk', ',') and language eq 'english'
"""
