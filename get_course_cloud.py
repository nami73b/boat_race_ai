import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from selenium import webdriver
from sqlalchemy import create_engine
import sys
import traceback

def get_page_source(url):
    html = urlopen(url)
    soup = BeautifulSoup(html,"html.parser")
    return soup

def get_course_list(race_date):
    url = 'https://www.boatrace.jp/owpc/pc/race/index?hd={}'
    url = url.format(race_date)
    soup = get_page_source(url)

    course_list = []
    for row in soup.find_all('td', {'class': 'is-arrow1 is-fBold is-fs15'}):
        course_list.append(row.find_all('img')[0].get('src').split('_')[-1].replace('.png', ''))
        
    return course_list

def main(request):
    request_json = request.get_json()
    date = request_json['race_date']
    
    dt = datetime.datetime(int(date[:4]),int(date[4:6]),int(date[6:8]))
    
    return str(get_course_list(dt.strftime('%Y%m%d'))).replace('[','').replace(']','')
