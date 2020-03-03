import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from selenium import webdriver
from sqlalchemy import create_engine
import sys
import traceback

def get_grade(class_names):
    if ['heading2_title', 'is-ippan'] in class_names:
        return '一般'
    elif ['heading2_title', 'is-G3b'] in class_names:
        return 'G3'
    elif ['heading2_title', 'is-G2b'] in class_names:
        return 'G2'
    elif ['heading2_title', 'is-G1b'] in class_names:
        return 'G1'
    elif ['heading2_title', 'is-SGa'] in class_names:
        return 'SG'
    else:
        np.nan

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
    # return create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={encoding}'.format(**db_settings))
    return create_engine('mysql+pymysql://{user}:{password}@/{database}?unix_socket=/cloudsql/{db_name}'.format(**db_settings))

def to_float(x):
    try:
        return float(x)
    except:
        return None

def to_int(x):
    try:
        return int(x)
    except:
        return None

def get_data_syussou_hyo(soup, race_date, place_id, race_no):
    race_dct = {}
    player_ar = []
    
    race_dct['race_date'] = race_date # 開催日付
    race_dct['place_id'] = place_id # 場コード
    race_dct['race_no'] = race_no # レース番号
    race_dct['race_name'] = soup.find_all('h2', {'class': 'heading2_titleName'})[0].get_text().replace('\u3000', ' ')
    race_dct['race_grade'] = get_grade([d.get('class') for d in soup.find_all('div')])
    
    race_class = ''
    for st in soup.find_all('span', {'class': 'heading2_titleDetail is-type1'})[0].get_text().split()[:-1]:
        race_class += st
    race_dct['race_class'] = race_class

    race_dct['distance'] = soup.find_all('span', {'class': 'heading2_titleDetail is-type1'})[0].get_text().split()[-1]
    
    for rows in soup.find_all('tbody', {'class': 'is-fs12'}):
        player_tmp = {}
        player_tmp['is_miss'] = 'False'
        player_tmp['bracket_no'] = str(to_int(rows.find_all('td')[0].get_text()))
        tmp_cell = rows.find_all('td')[2]
        player_tmp['race_date_no'] = race_date + "{0:02d}".format(int(race_no))
        player_tmp['player_id'] = tmp_cell.find_all('div', {'class': 'is-fs11'})[0].get_text().split()[0]
        player_tmp['player_grade'] = tmp_cell.find_all('span')[0].get_text()
        player_tmp['player_name'] = tmp_cell.find_all('div', {'class': 'is-fs18 is-fBold'})[0].find_all('a')[0].get_text().replace('\u3000', '')
        player_tmp['branch'] = tmp_cell.find_all('div', {'class': 'is-fs11'})[1].get_text().split()[0].split('/')[0]
        player_tmp['born_area'] = tmp_cell.find_all('div', {'class': 'is-fs11'})[1].get_text().split()[0].split('/')[1]
        player_tmp['age'] = to_int(tmp_cell.find_all('div', {'class': 'is-fs11'})[1].get_text().split()[1].split('/')[0].replace('歳', ''))
        player_tmp['f_count'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[0].get_text().split()[0].replace('F', ''))
        player_tmp['l_count'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[0].get_text().split()[1].replace('L', ''))
        player_tmp['start_time_avg'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[0].get_text().split()[2])
        player_tmp['first_rate_all'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[1].get_text().split()[0])
        player_tmp['second_rate_all'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[1].get_text().split()[1])
        player_tmp['third_rate_all'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[1].get_text().split()[2])
        player_tmp['first_rate_area'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[2].get_text().split()[0])
        player_tmp['second_rate_area'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[2].get_text().split()[1])
        player_tmp['third_rate_area'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[2].get_text().split()[2])
        player_tmp['motor_no'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[3].get_text().split()[0])
        player_tmp['motor_within_second_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[3].get_text().split()[1])
        player_tmp['motor_within_third_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[3].get_text().split()[2])
        player_tmp['boat_no'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[4].get_text().split()[0])
        player_tmp['boat_within_second_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[4].get_text().split()[1])
        player_tmp['boat_within_third_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[4].get_text().split()[2])
        
        player_ar.append(player_tmp)
        
    for rows in soup.find_all('tbody', {'class': 'is-miss is-fs12'}):
        player_tmp = {}
        player_tmp['is_miss'] = 'True'
        player_tmp['bracket_no'] = str(to_int(rows.find_all('td')[0].get_text()))
        tmp_cell = rows.find_all('td')[2]
        player_tmp['player_id'] = tmp_cell.find_all('div', {'class': 'is-fs11'})[0].get_text().split()[0]
        player_tmp['player_grade'] = tmp_cell.find_all('span')[0].get_text()
        player_tmp['player_name'] = tmp_cell.find_all('div', {'class': 'is-fs18 is-fBold'})[0].find_all('a')[0].get_text().replace('\u3000', '')
        player_tmp['branch'] = tmp_cell.find_all('div', {'class': 'is-fs11'})[1].get_text().split()[0].split('/')[0]
        player_tmp['born_area'] = tmp_cell.find_all('div', {'class': 'is-fs11'})[1].get_text().split()[0].split('/')[1]
        player_tmp['age'] = to_int(tmp_cell.find_all('div', {'class': 'is-fs11'})[1].get_text().split()[1].split('/')[0].replace('歳', ''))
        player_tmp['f_count'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[0].get_text().split()[0].replace('F', ''))
        player_tmp['l_count'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[0].get_text().split()[1].replace('L', ''))
        player_tmp['start_time_avg'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[0].get_text().split()[2])
        player_tmp['first_rate_all'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[1].get_text().split()[0])
        player_tmp['second_rate_all'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[1].get_text().split()[1])
        player_tmp['third_rate_all'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[1].get_text().split()[2])
        player_tmp['first_rate_area'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[2].get_text().split()[0])
        player_tmp['second_rate_area'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[2].get_text().split()[1])
        player_tmp['third_rate_area'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[2].get_text().split()[2])
        player_tmp['motor_no'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[3].get_text().split()[0])
        player_tmp['motor_within_second_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[3].get_text().split()[1])
        player_tmp['motor_within_third_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[3].get_text().split()[2])
        player_tmp['boat_no'] = to_int(rows.find_all('td', {'class': 'is-lineH2'})[4].get_text().split()[0])
        player_tmp['boat_within_second_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[4].get_text().split()[1])
        player_tmp['boat_within_third_rate'] = to_float(rows.find_all('td', {'class': 'is-lineH2'})[4].get_text().split()[2])
        
        player_ar.append(player_tmp)
        
    return [race_dct, player_ar]

def get_data_justbefore(soup, race_date, place_id, race_no):
    player_ar = []
    player_tmp = {}
    race_dct = {}
    pre_start_dct = {}
    bracket_order = {}
    race_dct['course_direction'] = soup.find_all('div', {'class': 'weather1_bodyUnit is-direction'})[0].find_all('p')[0].get('class')[-1].replace('is-direction', '')
    race_dct['weather'] = soup.find_all('span', {'class': 'weather1_bodyUnitLabelTitle'})[1].get_text()
    race_dct['temperature'] = soup.find_all('span', ('class', 'weather1_bodyUnitLabelData'))[0].get_text()
    race_dct['wind'] = soup.find_all('span', ('class', 'weather1_bodyUnitLabelData'))[1].get_text()
    race_dct['wind_direction'] = soup.find_all('div', {'class': 'weather1_bodyUnit is-windDirection'})[0].find_all('p')[0].get('class')[-1].replace('is-wind', '')
    race_dct['water_temperature'] = soup.find_all('span', ('class', 'weather1_bodyUnitLabelData'))[2].get_text()
    race_dct['wave_height'] = soup.find_all('span', ('class', 'weather1_bodyUnitLabelData'))[3].get_text()
    
    for c, pre in enumerate(soup.find_all('tbody', {'class': 'is-p10-0'})[0].find_all('tr')):
        pre_start = pre.get_text().split()
        if len(pre_start) > 0:
            bracket_order[pre_start[0]] = c+1
            if pre_start[0] != '':
                pre_start_dct[pre_start[0]] = pre_start[-1]
        else:
            pass
        for i in range(6):
            if str(i+1) not in list(pre_start_dct.keys()):
                pre_start_dct[str(i+1)] = 'NULL'
            if str(i+1) not in list(bracket_order.keys()):
                bracket_order[str(i+1)] = 'NULL'
    
    for rows, pre in zip(soup.find_all('tbody', {'class': 'is-fs12 '}), soup.find_all('tbody', {'class': 'is-p10-0'})[0].find_all('tr')):
        player_tmp = {}
        player_tmp['bracket_no'] = rows.find_all('td')[0].get_text()
        player_tmp['weight'] = to_float(rows.find_all('td')[3].get_text().replace('kg', ''))
        player_tmp['pre_time'] = to_float(rows.find_all('td')[4].get_text())
        player_tmp['tilt_angle'] = to_float(rows.find_all('td')[5].get_text())
        player_tmp['propeller'] = rows.find_all('td')[6].get_text().replace('\xa0', '')
        player_tmp['parts'] = rows.find_all('td')[7].get_text().replace('\n', '')
        player_tmp['adjust_weight'] = to_float(rows.find_all('tr')[2].find_all('td')[0].get_text())
        player_tmp['pre_start_timing'] = pre_start_dct[player_tmp['bracket_no']]
        player_tmp['bracket_order'] = bracket_order[player_tmp['bracket_no']]
        
        player_ar.append(player_tmp)
    return [race_dct, player_ar]

def get_data_result(soup, race_date, place_id, race_no):
    player_ar = []
    player_tmp = {}
    race_dct = {}
    
    table = soup.find_all('table')[3]
    for row, bet_type in zip(table.find_all('tbody'), ['trifecta', 'trio', 'exacta', 'quinella', 'wide', 'win', 'place']):
        tmp = []
        for text in row.find_all('div', {'class': 'numberSet1_row'}):
            if text.get_text().replace('\n', '') != '':
                tmp.append(text.get_text().replace('\n', ''))
        race_dct[bet_type+'_cmb'] = tmp
        tmp = []
        for text in row.find_all('span', {'class': 'is-payout1'}):
            if text.get_text().replace('\n', '') != '\xa0':
                tmp.append(text.get_text().replace('\n', ''))
        race_dct[bet_type+'_payoff'] = tmp
    
    start_dct = {}
    for bracket, start in zip(soup.find_all('span', {'class': 'table1_boatImage1Number'}), soup.find_all('span', {'class': 'table1_boatImage1Time'})):
        start_dct[bracket.get_text()] = start.get_text().split() if len(start.get_text().split()) == 2 else start.get_text().split() + ['']
    for i in range(6):
            if str(i+1) not in list(start_dct.keys()):
                start_dct[str(i+1)] = ['NULL', 'NULL']
        
    table = soup.find_all('table', {'class': 'is-w495'})[0]
    for row in table.find_all('tbody'):
        player_tmp = {}
        player_tmp['bracket_no'] = row.find_all('td')[1].get_text()
        player_tmp['finish_order'] = to_int(row.find_all('td', {'class', 'is-fs14'})[0].get_text())
        player_tmp['player_race_time'] = row.find_all('td')[3].get_text().replace('\'', ':').replace('"', ':')
        player_tmp['start_timing'] = start_dct[row.find_all('td')[1].get_text()][0]
        player_tmp['win_pattern'] = start_dct[row.find_all('td')[1].get_text()][1]
        
        player_ar.append(player_tmp)

    return [race_dct, player_ar]

def get_course_list(race_date):
    url = 'https://www.boatrace.jp/owpc/pc/race/index?hd={}'
    url = url.format(race_date)
    soup = get_page_source(url)

    course_list = []
    for row in soup.find_all('td', {'class': 'is-arrow1 is-fBold is-fs15'}):
        course_list.append(row.find_all('img')[0].get('src').split('_')[-1].replace('.png', ''))
        
    return course_list

def get_page_source(url):
    html = urlopen(url)
    soup = BeautifulSoup(html,"html.parser")
    return soup

def preprocessing_race_data(race_data):
    # 距離を数値に変換
    race_data['distance'] = to_int(race_data['distance'].replace('m', ''))
    # 気温を数値に変換
    race_data['temperature'] = to_float(race_data['temperature'].replace('℃', ''))
    # 風量を数値に変換
    race_data['wind'] = to_int(race_data['wind'].replace('m', ''))
    # 水温を数値に変換
    race_data['water_temperature'] = to_float(race_data['water_temperature'].replace('℃', ''))
    # 波高を数値に変換
    race_data['wave_height'] = to_float(race_data['wave_height'].replace('cm', ''))

    return race_data

def preprocessing_player_data(player_data):
    # あとで考えるかも
    return player_data

def insert_db(data, table_name, engine):
    col_txt = ''
    value_txt = ''

    for key, value in data.items():
        col_txt += key + ','
        if type(value) == str:
            value_txt += "'"
        value_txt += str(value)
        if type(value) == str:
            value_txt += "'"
        value_txt += ','
    col_txt = col_txt[:-1]
    value_txt = value_txt[:-1]

    insert_txt = 'insert into '+table_name+' (' + col_txt + ') values('+value_txt+')'
    insert_txt = insert_txt.replace('None', 'NULL')
    
    engine.execute(insert_txt)

def insert_db_payoff(payoff_data, engine):
    # 三連単
    sub_number = 0
    for cmb, payoff in zip(payoff_data['trifecta_cmb'], payoff_data['trifecta_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',7,"+str(sub_number)+','
        insert_txt += cmb.split('-')[0]+','+cmb.split('-')[1]+','+cmb.split('-')[2]+','+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

    # 三連複
    sub_number = 0
    for cmb, payoff in zip(payoff_data['trio_cmb'], payoff_data['trio_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',6,"+str(sub_number)+','
        insert_txt += cmb.split('=')[0]+','+cmb.split('=')[1]+','+cmb.split('=')[2]+','+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

    # 二連単
    sub_number = 0
    for cmb, payoff in zip(payoff_data['exacta_cmb'], payoff_data['exacta_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',5,"+str(sub_number)+','
        insert_txt += cmb.split('-')[0]+','+cmb.split('-')[1]+',null,'+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

    # 二連複
    sub_number = 0
    for cmb, payoff in zip(payoff_data['quinella_cmb'], payoff_data['quinella_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',4,"+str(sub_number)+','
        insert_txt += cmb.split('=')[0]+','+cmb.split('=')[1]+',null,'+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

    # ワイド
    sub_number = 0
    for cmb, payoff in zip(payoff_data['wide_cmb'], payoff_data['wide_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',3,"+str(sub_number)+','
        insert_txt += cmb.split('=')[0]+','+cmb.split('=')[1]+',null,'+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

    # 複勝
    sub_number = 0
    for cmb, payoff in zip(payoff_data['place_cmb'], payoff_data['place_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',2,"+str(sub_number)+','
        insert_txt += cmb+',null,null,'+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

    # 単勝
    sub_number = 0
    for cmb, payoff in zip(payoff_data['win_cmb'], payoff_data['win_payoff']):
        insert_txt = 'insert into race_payoff (race_date, place_id, race_no, bet_type, sub_number, bracket1, bracket2, bracket3, payoff)'
        insert_txt += "values('"+payoff_data['race_date']+"','"+payoff_data['place_id']+"','"+payoff_data['race_no']+"',1,"+str(sub_number)+','
        insert_txt += cmb+',null,null,'+payoff.replace(',','').replace('¥', '')+')'
        sub_number += 1
        insert_txt = insert_txt.replace('None', 'NULL')
        
        engine.execute(insert_txt)

def main(request):
    request_json = request.get_json()
    date = request_json['race_date']
    place_id = request_json['place_id']
    race_no = request_json['race_no']
    
    dt = datetime.datetime(int(date[:4]),int(date[4:6]),int(date[6:8]))
    
    # DB接続
    engine = load_engine()

    # 出走表URL
    syussou_hyo_url = 'https://boatrace.jp/owpc/pc/race/racelist?rno={race_no}&jcd={place_id}&hd={race_date}'
    # 直前情報URL
    justbefore_url = 'https://boatrace.jp/owpc/pc/race/beforeinfo?rno={race_no}&jcd={place_id}&hd={race_date}'
    # 結果情報URL
    result_url = 'http://boatrace.jp/owpc/pc/race/raceresult?rno={race_no}&jcd={place_id}&hd={race_date}'



    
    race_data = {}
    player_data = {'1': {}, '2': {}, '3': {}, '4': {}, '5': {}, '6': {}}

    url = syussou_hyo_url.format(**{'race_date': dt.strftime('%Y%m%d'), 'race_no': race_no, 'place_id': place_id})
    soup = get_page_source(url)

    # 開催データがない場合の処理

    race_date_tmp, player_data_tmp = get_data_syussou_hyo(soup, dt.strftime('%Y%m%d'), place_id, race_no)
    for key, value in race_date_tmp.items():
        race_data[key] = value
    for player in player_data_tmp:
        bracket_no = player['bracket_no']
        for key, value in (player.items()):
            player_data[bracket_no][key] = value

    url = justbefore_url.format(**{'race_date': dt.strftime('%Y%m%d'), 'race_no': race_no, 'place_id': place_id})
    soup = get_page_source(url)

    race_date_tmp, player_data_tmp = get_data_justbefore(soup, dt.strftime('%Y%m%d'), place_id, race_no)
    for key, value in race_date_tmp.items():
        race_data[key] = value
    for player in player_data_tmp:
        bracket_no = player['bracket_no']
        for key, value in (player.items()):
            player_data[bracket_no][key] = value

    url = result_url.format(**{'race_date': dt.strftime('%Y%m%d'), 'race_no': race_no, 'place_id': place_id})
    soup = get_page_source(url)

    payoff_data, player_data_tmp = get_data_result(soup, dt.strftime('%Y%m%d'), place_id, race_no)

    for player in player_data_tmp:
        bracket_no = player['bracket_no']
        for key, value in (player.items()):
            player_data[bracket_no][key] = value

    # キー情報
    payoff_data['race_date'] = race_data['race_date']
    payoff_data['place_id'] = race_data['place_id']
    payoff_data['race_no'] = race_data['race_no']

    race_data = preprocessing_race_data(race_data)
    
    player_data = preprocessing_player_data(player_data)
    
    insert_db(race_data, 'race_detail', engine)
    for i in range(6):
        player_data[str(i+1)]['race_date'] = race_data['race_date']
        player_data[str(i+1)]['place_id'] = race_data['place_id']
        player_data[str(i+1)]['race_no'] = race_data['race_no']

        insert_db(player_data[str(i+1)], 'race_player', engine)
    
    insert_db_payoff(payoff_data, engine)
