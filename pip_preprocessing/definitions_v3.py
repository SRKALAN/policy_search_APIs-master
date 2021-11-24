import pandas as pd
from bs4 import BeautifulSoup
import re
import time
from statistics import mode,mean
import os
from tika import parser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter, XMLConverter, HTMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import BytesIO
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import os, uuid, json,re
import itertools
import copy,uuid
import pandas as pd
import numpy as np
from statistics import mean
import traceback
import time
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter, XMLConverter, HTMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage


sectionRE=re.compile('Section\s+(?:\d+\D*|[A-Z]+)\s+')
sectionRECaps=re.compile('SECTION\s+(?:\d+|[A-Z]+)\s+')
pageNumRE=re.compile('Page\s*(\d{1,3})',re.IGNORECASE)
pointsRE_heading=re.compile('(?:\s*\([a-z]{1,3}\)|[A-Z]{1}\s+[a-zA-Z0-9_\s]{5})')
stop_words = list(set(stopwords.words('english')))
y_cordinate_Re=re.compile('top:(\d{1,5})px')
height_re=re.compile('height:(\d{1,5})px')
num_point_RE=re.compile(r'\d{1,2}[.]\d{0,2}[.]*\d{0,2}[.]*\d{0,2}\s*')
alpha_point_RE=re.compile(r'[\t\n][\(]*[a-z]{1,2}[.\)]\s')
alpha_point__spaceRE=re.compile(r'[\t\n][\(]*[a-z]{1,2}[.\)]*\s\s')
# asDefinedRe=re.compile('\as defined in section\s+\d*\D*\)',re.IGNORECASE)
pointsRE=re.compile('(\s*\([a-z]{1,3}\))')
alphabetcheckRE=re.compile('[a-z]',re.IGNORECASE)

SubHeaders=['exclusion','condition','cover','willpay','willnotpay','definition']
HeadersExculded=['exclusion','conditions','willpay','willnotpay','definition']
footnotechar=['TM','chubb','page']
bag_subhead=['notcover','notpay','exclusion','condition']

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
import io
import os
def convert_page(path):
    text=[]
    fp = open(path, 'rb')
    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
#     print(type(retstr))
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    page_no = 0
    for pageNumber, page in enumerate(PDFPage.get_pages(fp)):
        if pageNumber == page_no:
            interpreter.process_page(page)
#             print(page_no)
            data = retstr.getvalue()

            text.append((data,page_no+1))
            data = ''
            retstr.truncate(0)
            retstr.seek(0)

        page_no += 1
    return(text)
   
def convert_pdf(path, format='text', codec='utf-8', password=''):
    rsrcmgr = PDFResourceManager()
    retstr = BytesIO()
    laparams = LAParams()
    if format == 'text':
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    elif format == 'html':
        device = HTMLConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    elif format == 'xml':
        device = XMLConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    else:
        raise ValueError('provide format, either text, html or xml!')
    fp = open(path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    maxpages = 0
    caching = True
    pagenos=set()
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=False):
        interpreter.process_page(page)

    text = retstr.getvalue().decode()
    fp.close()
    device.close()
    retstr.close()
    return text

def clean(item):
    if isinstance(item,str):
        return item.lower().replace(' ','').replace('\n','').rstrip().lstrip()
    elif isinstance(item,list):
        return [i.lower().replace(' ','').rstrip().replace('\n','').lstrip() for i in item ]
        
def combine_dict(dict_list):
    all_definitions={}
    for dictionary in dict_list:
        for key,value in dictionary.items():
            if key in all_definitions:
 
                if len(value[0]) > len(all_definitions[key][0]):
                    all_definitions[key]=value
            else:
                all_definitions[key]=value
    return all_definitions
                
                    
def replace_colnword(text):
    colnwords=['shall mean','means 100% of','means any','means']
    for word in colnwords:
        regex_='{}[;:,]'.format(word)
        text=re.sub(regex_,word,text)
    return text
        
def replace_bulletpts(text):
    
    count=len(num_point_RE.findall(text))+len(alpha_point_RE.findall(text))+len(alpha_point__spaceRE.findall(text))
   
    text=num_point_RE.sub('',text)
    text=alpha_point_RE.sub('',text)
    text=alpha_point__spaceRE.sub('',text)

    return text,count

def get_def_from_line_list(def_list,page,def_count_on_page):
    def_dict={}

    for defn in def_list:

        
        defn=re.sub("\(.*?\)",'',defn)
        
        word_list=defn.strip().split(' ')
        
        
#         word_list=[i for i in word_list_1 if len(i)>1]
        try:
            if 'means' in defn:
                pos=word_list.index('means')
            if 'shall mean' in defn:
                pos=word_list.index('shall')

            key=' '.join(word_list[:pos])
            key = re.sub(r'\d+', '', key).strip()
            key=key.replace('\n',' ').strip()
            key=re.sub(r'[B-Z][;:,)]*\s+','',key).strip()
            value=' '.join(word_list[pos:]).strip().replace('\n',' ')
            if def_count_on_page < 3 :
            	word_count_limit_defn=200
            else:
            	word_count_limit_defn=500

            if len(word_list)>word_count_limit_defn:
            	# Find all the 2 space gaps
                inilist = [m.start() for m in re.finditer(r"  ", value)]
                # Find all the gaps after 300 words
                inilist=[i for i in inilist if i >200 ]
                if inilist:
                    space_pos=min(inilist)
                    value=value[:space_pos]
                
            
            val=(value,page)
        except:
            continue
 
        if pos<8:
            def_dict.update({key:val})
        elif pos< 12 and ',' in key:
            key=key[:key.find(',')]
            def_dict.update({key:val})
      

            
            
    return def_dict
    
    
def stopword_check(word,text):
    stop_words = list(set(stopwords.words('english')))
    transition="although  instead  whereas  despite  conversely  otherwise  however moreover  likewise  comparatively  correspondingly  similarly  furthermore  additionallyver  rather  nevertheless  nonetheless  regardless  notwithstanding consequently  therefore  thereupon  forthwith  accordingly  henceforth"
    transition_words=transition.split()
    transition_words
    stop_words.extend(transition_words)

    if def_check(text):
        return True
    if word.lower() in stop_words:
        return False
    else:
        return True

def means_to_means_split(text):
    textlist=text.replace('\n',' ').split('.')
    textlist_m_to_m=[]
    def_start=False
    def_text=''
    for line in textlist:
        if 'means' in line or 'shall mean'  in line:
            def_start=True
#             print('saving...',def_text,'\n')
#             if 'forcible' in def_text:
#                 print(def_text)
            
            
            textlist_m_to_m.append(def_text)
                
            def_text=line
#             print('starting...',def_text)
        else:
            if def_start:
                def_text+=' '+line
#                 print('Appending',line,'\n')
#     print('saving...',def_text,'\n')
    textlist_m_to_m.append(def_text)

    return textlist_m_to_m
                
            
        

def def_extraction(file):
    maxpage=0
    full_text=''
    def_page_lis=convert_page(file)
    def_dict_1={}
    def_dict_2={}
    for line,page in def_page_lis:
        full_text+=line.replace('\n',' ')
#         print(page)
        doc_text=replace_colnword(line)
        doc_text=re.sub(';|:','.',doc_text)

        def_list=[defs for defs in doc_text.replace('\n\n','.').split('.') if any(['means' in defs,'shall mean' in defs])] 

        def_count_on_page=len(def_list)

        def_dict=get_def_from_line_list(def_list,page,def_count_on_page)
        
        def_dict_1.update(def_dict)

        doc_text,count=replace_bulletpts(line)

        doc_text=replace_colnword(doc_text)
        doc_text=re.sub(';|:','.',doc_text)

        if count <4 :     
            def_list=[defs for defs in (doc_text.replace('\n',' ')).split('.') if  any(['means' in defs,'shall mean' in defs])]
        else:

            def_list=means_to_means_split(doc_text)
        def_count_on_page=len(def_list)

        def_dict=get_def_from_line_list(def_list,page,def_count_on_page)
#         print('def dict 2!!!!!',def_dict)
        def_dict_2.update(def_dict)
        maxpage=page
    
    
    all_definitions = combine_dict([def_dict_1,def_dict_2])

    # for key, value in all_definitions.items():
    count=len(re.findall(r"mean", full_text.lower()))
#     print(count,'FULLTEXT')
    if len(all_definitions)<10 and maxpage>10 and count<4:
        html_def_pages=html_extraction(file)
        for line,pgno in def_page_lis:
            if pgno in html_def_pages:
                all_definitions['Definitions '+str(pgno)]=(line.replace('\n',''),pgno)
    all_definitions={ ('no_name_found' if len(key)<3 else key):(value) for key, value in all_definitions.items() }
    return(all_definitions)
 
 
 
def html_extraction(file):
    definition_text=''
    def_pages=[]
    header_match_object=(0,'',False)
    cond_header_match_object=(0,'',False)
    second_category=False
    head_found=False
    def_flag=False
    cond_head_found=False
    single_page_head_found=False
    cond_single_page_head_found=False
    try:
        html=convert_pdf(file,'html')
        previous_span=''
        soup = BeautifulSoup(html, 'html5lib')
    #     print(soup,'osup')
    except:
        return([])
    underline_positions=get_underlines(soup)
    fontsizes=font_extraction(soup)
    if fontsizes:
        file_font_size_mode=mode(fontsizes)
    else:
        file_font_size_mode=8

    bold=False
    italic=False
    for divs in soup.findAll('div'):
        bold_lis=[]
        div_text_list=[span.text for span in divs.find_all('span') ]
        page_str=str(divs.find_all('a'))
        page_num_results=pageNumRE.findall(page_str)
        if page_num_results:
            pagenum=page_num_results[0]
        for span in divs.find_all('span'):
            bold=False
            italic=False
            upper=False
            bullet=False
            def_flag=False
            span_position=div_text_list.index(span.text)

            if "Bold" in str(span) or 'CIDFont+F3' in str(span):
                bold_lis.append(span.text)
                bold=True
            if "Italic" in str(span):
                italic=True
#                 print(str(span))
            if span.text.isupper():
                upper=True
            font_family_match=re.findall(r"font-family: b'(.*)';",str(span))
            if font_family_match:
                font_family=font_family_match[0]
            else:
                font_family=''
#                     print(font_family)
            font_size_match=re.findall(r'font-size:(.*)px">',str(span))
            if font_size_match:
                font_size=int(font_size_match[0])
            if pointsRE_heading.findall(span.text):
                bullet=True
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
            y_cord_list=y_cordinate_Re.findall(str(divs))
            if y_cord_list:
                y_cord=int(y_cord_list[0])
            underlined_text=check_underline(underline_positions,y_cord)
            if span.text.split('\n')[0]!='' and span.text.split('\n')[0]!=' ':
                head_check_text=span.text.split('\n')[0]
            elif len(span.text.split('\n'))>1:
                head_check_text=span.text.split('\n')[1]
            else:
                head_check_text=''
##############################3  
            if str(previous_span).endswith('<br/></span>'):
                span_position=0
            ##################INSERT FUNCTION####################z
            #EXCLUSION 
            if  span_position==0 and (bold or italic or font_size>= file_font_size_mode +2 ) and (font_size> file_font_size_mode or upper) and 5<len(head_check_text.strip())<80:                    
                if def_check(head_check_text) and not head_found: 
                    head_found=True
                    header_match_object=(font_size,font_family,bold,italic,upper,bullet)
                    First_category=True
                    second_category=False
                    single_page_head_found=True
#                     print('FOUND Heading...',span.text)
                    def_pages.append(int(pagenum))
#                     print(pagenum)

                elif ((font_size,font_family,bold,italic,upper,bullet)==header_match_object and not def_check(head_check_text) or font_size>header_match_object[0] ) and head_found:
                    head_found=False
                    single_page_head_found=True
#                     print('FOUND Closure...',span.text)
                    def_pages.append(int(pagenum))
#                     print(pagenum)

            words_title=[word.istitle() for word in head_check_text.split() if stopword_check(word,head_check_text) and not word.isdigit() ]
#             if italic:
#                 print(span_position==0,all(words_title),head_check_text.strip(),(len(words_title) >=1 or underlined_text ), 5<len(head_check_text.strip())<80)
            if (span_position==0 and all(words_title) and head_check_text.strip() and  (len(words_title) >=1 or underlined_text )and 5<len(head_check_text.strip())<80 ):
#                 print('MMMMMMMMARARARA',span.text)
#                 if 'italic':
#                     print(span.text)
#                     print(def_check(head_check_text),head_found)
                if def_check(head_check_text) and  not head_found:
                    head_found=True
                    second_category=True
                    First_category=False
                    single_page_head_found=True
                    header_match_object=(font_size,font_family,bold,italic,upper,bullet)
#                     print("FOUND heading type2 ....",span.text)
                    def_pages.append(int(pagenum))
                elif head_found and not def_check(head_check_text) and second_category and ((font_size,font_family,bold,italic,upper,bullet)==header_match_object or font_size > header_match_object[0] ) :
                    head_found=False
                    second_category=False
                    single_page_head_found=True
#                     print('FOUND closure type 2',span.text)
                    def_pages.append(int(pagenum))
    return(list(set(def_pages)))



def def_check(text):
    def_bag=['definitions']
    for word in def_bag:
        if word in text.lower():
            return True
    return False
    
def font_extraction(soup):
    fontsizes=[]
    for divs in soup.findAll('div'):
        for j in divs.find_all('span'):
            ext_size=re.findall(r'font-size:(.*)px">',str(j))
            if ext_size:
                fontsizes.append(int(ext_size[0]))
    return(fontsizes)

def get_underlines(soup):
    positions=[]
    for span in soup.find_all('span'):
        y_cord_list=y_cordinate_Re.findall(str(span))
        if y_cord_list:
            y_cord=int(y_cord_list[0])
        else:
            continue
        style="position:absolute; border: black 1px solid" in str(span)
        height_px_li=height_re.findall(str(span))
        if height_px_li:
            height_px=height_px_li[0]
        else:
            continue
        height=int(height_px)<15
        if all([style,height]):
            positions.append(y_cord)
    return list(set(positions))

def check_underline(positions,y_cord_text):
    positions=[i for i in positions if y_cord_text<=i< y_cord_text+16]
    if positions:
        return True
    else:
        return False
    