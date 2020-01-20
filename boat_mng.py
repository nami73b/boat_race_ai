import datetime
import time
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import sys
import traceback
import threading

from predict import Predict

def get_race_inf():
    url = 'https://www.boatrace.jp/owpc/pc/race/index'
    html = urlopen(url)
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
    
    return race_list

def thred_tsk(race_date, place_id, race_no):
    predict = Predict(race_date, place_id, race_no)
    predict.main_proess()

def main_process():
    while True:
        print(datetime.datetime.now())
        race_list = get_race_inf()
        print(race_list)
        for race in race_list:
            if race['vote_deadline'] == (datetime.datetime.now()+datetime.timedelta(minutes=5)).strftime('%H:%M'):
                print('Predict!!')
                thread = threading.Thread(target=thred_tsk, args=([datetime.datetime.now().strftime('%Y%m%d'), race['place_id'], race['race_no']]))
                thread.start()
                
        time.sleep(60)
        
        
    


if __name__ == '__main__':
    main_process()
    