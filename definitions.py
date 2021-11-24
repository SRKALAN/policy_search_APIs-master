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


# asDefinedRe=re.compile('\as defined in section\s+\d*\D*\)',re.IGNORECASE)
pointsRE=re.compile('(\s*\([a-z]{1,3}\))')
alphabetcheckRE=re.compile('[a-z]',re.IGNORECASE)

SubHeaders=['exclusion','condition','cover','willpay','willnotpay','definition']
HeadersExculded=['exclusion','conditions','willpay','willnotpay','definition']
footnotechar=['TM','chubb','page']
bag_subhead=['notcover','notpay','exclusion','condition']



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
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
        interpreter.process_page(page)

    text = retstr.getvalue().decode()
    fp.close()
    device.close()
    retstr.close()
    return text



def get_html_defs(html,def_string):
    currentSubHead=None
    prevLine=['','','','']
    currentHead=None
    AllHeadings={}
    reqHeadings={}
    text=[]
    DefRE=re.compile(def_string,re.IGNORECASE )
    pageNum=0
    cc=[]
    productNameFound=False
    soup = BeautifulSoup(html, 'html5lib')
    for divs in soup.findAll('div'):
        for j in divs.find_all('span'):

            if clean(j.text)=='\u2013' or '©' in j.text:
                continue
            elif '\u2013' in clean(j.text) and len(clean(j.text))>10:
                text=j.text.replace('\u2013','')
            else:
                text=j.text.replace(':','.')

            if 'Bold' in str(j):
                text=j.text.replace('\n',' ').strip()
                currentHead=text
                AllHeadings[currentHead]=[]
            else:
                if currentHead:
                    text=text.replace('\n',' ').strip()
                    AllHeadings[currentHead].append((text))

#             if currentHead!=None and currentHead not in AllHeadings:
#                 AllHeadings[currentHead].append((text))

    return(AllHeadings)




def returnDefinitions(Alldict,LossRE):
    defs_dict={k:"".join(" ".join(v).split('.')) for k,v in Alldict.items() if (v and v[0].startswith('means')) or (k.endswith('means'))}
#     loss_dict = [k:(" ".join(v)).split('.') for k,v in Alldict.items() if LossRE in k]
    return(defs_dict)




def def_extraction(file):
    def_dict={}
#     def_dict={}
    doc_text=convert_pdf(file)
    def_list=[defs for defs in (doc_text.replace('\n',' ')).split('.') if 'means' in defs]
    for defn in def_list:
        word_list=defn.split(' ')
        try:
            pos=word_list.index('means')
            key=' '.join(word_list[:pos])
            key = re.sub(r'\d+', '', key).strip()
            value=' '.join(word_list[pos:]).strip()
        except:
            continue
        if pos<7:
            def_dict.update({key:value})
#                 print(defn)
#             print(defn)
    if len(def_dict.keys())<20:
        print(def_dict.keys())
        html=convert_pdf(file,'html')
        html_doc=get_html_defs(html,"Definition")
        def_dict=returnDefinitions(html_doc,'means')
        print(len(def_dict.keys()))
    return(def_dict)




def clean(item):
    if isinstance(item,str):
        return item.lower().replace(' ','').replace('\n','').rstrip().lstrip()
    elif isinstance(item,list):
        return [i.lower().replace(' ','').rstrip().replace('\n','').lstrip() for i in item ]
