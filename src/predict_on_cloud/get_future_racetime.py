import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import sys
import traceback

INDEX_URL = 'https://www.boatrace.jp/owpc/pc/race/index'

def get_future_racetime():
    html = urlopen(INDEX_URL)
    soup = BeautifulSoup(html,"html.parser")
    
    table = soup.find_all('div', {'class': 'table1'})[0].find_all('table')[0]
    rows = table.find_all('tbody')
    rows = list(filter(lambda x: str(x).find('発売中')!=-1, rows))
    
    race_list = []
    for row in rows:
        race_inf = {}
        race_inf['place_id'] = row.find_all('a')[0].get('href').split('(')[-1].split(',')[1].replace("'tv", '').replace("'", '')
        race_inf['race_no'] = row.find_all('td')[2].get_text().replace('R', '')
        race_inf['vote_deadline'] = row.find_all('tr')[1].find_all('td')[1].get_text()
        race_list.append(race_inf)
    race_list.sort(key=lambda x: x['vote_deadline'])
    
    return str(race_list).replace('[','').replace(']','').replace(' ', '')