import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from selenium import webdriver
from sqlalchemy import create_engine
import sys
import traceback

ex_return = 2000

def load_engine():
    db_settings = {
        "host": '34.85.101.153',
        "database": 'boatrace',
        "user": 'user',
        "password": 'mazikayo',
        "port": '3306',
        "encoding": "utf8",
        "db_name": "maximal-boulder-268803:asia-northeast1:boatrace-mysql"
    }
    return create_engine('mysql+pymysql://{user}:{password}@/{database}?unix_socket=/cloudsql/{db_name}'.format(**db_settings))

def get_ex_value(race_date, place_id, race_no, output):
    url = 'https://www.boatrace.jp/owpc/pc/race/oddstf?rno={race_no}&jcd={place_no}&hd={race_date}'
    url = url.format(**{'race_date': race_date ,'place_no': "{0:02d}".format(int(place_id)), 'race_no': race_no})
    html = urlopen(url)
    soup = BeautifulSoup(html,"html.parser")
    odds_win = get_odds_win(soup)
    
    ex_value = {}
    for pred, odds in zip(output, odds_win.items()):
        ex_value[odds[0]] = [pred, odds[1], pred*odds[1]]
    return ex_value
        
def get_odds_win(soup):
    odds = {}
    rows = soup.find_all('table', {'class': 'is-w495'})[0].find_all('tbody')
    for row in rows:
        cell = row.find_all('td')
        if cell[2].get_text() != '欠場':
            odds[cell[0].get_text()] = float(cell[2].get_text())
    return odds

def select_predict_result(race_date, place_id, race_no, engine):
    query = """
                select * from predict_result where race_date = {} and place_id = {} and race_no = {}
                order by bracket_no_1
            """
    result = engine.execute(query.format(race_date, place_id, race_no))
    output = [r[8] for r in result]
    return output

def insert_bet(race_date, place_id, race_no, ex_value,engine):
    i = 0
    print(ex_value.items())
    for key, ex in ex_value.items():
        if ex[2] > 1 and ex[0] > 0.02:
            query = """
                        insert into bet (race_date, place_id, race_no, bet_type, sub_number, bracket_no_1, pred, amount, odds, betting_time)
                        value({race_date}, {place_id}, {race_no}, {bet_type}, {sub_number}, {bracket_no_1}, {pred}, {amount}, {odds}, now())
                    """
            data = {
                        'race_date': race_date,
                        'place_id': place_id,
                        'race_no': race_no,
                        'bet_type': 1,
                        'sub_number': i,
                        'bracket_no_1': key,
                        'pred': ex[0],
                        'amount' : 1 if int(ex_return/(ex[1]*100)) == 0 else int(ex_return/(ex[1]*100)),
                        'odds': ex[1]
                    }
            print(data)
            engine.execute(query.format(**data))
            i += 1

def main(request):
    request_json = request.get_json()
    date = request_json['race_date']
    place_id = request_json['place_id']
    race_no = request_json['race_no']
    
    engine = load_engine()
    output = select_predict_result(date, place_id, race_no, engine)
    ex_value = get_ex_value(date, place_id, race_no, output)
    
    insert_bet(date, place_id, race_no, ex_value,engine)
    
    return str(ex_value)