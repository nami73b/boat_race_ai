import pandas as pd
import numpy as np
import copy
import os
import lightgbm as lgb
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from IPython.display import clear_output
from urllib.request import urlopen
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import math
import gc
import sys
from io import BytesIO
import time
import traceback

from google.cloud import storage as gcs

LOAD_DATA_COLUMNS = ['race_date', 'place_id', 'race_no', 'race_date_no', 'bracket_no', 'is_miss', 'player_id', 'player_grade', 'branch', 'born_area', 'age', 'weight', 'f_count', 'l_count', 'start_time_avg', 'first_rate_all', 'second_rate_all', 'third_rate_all', 'first_rate_area', 'second_rate_area', 'third_rate_area', 'motor_no', 'motor_within_second_rate', 'motor_within_third_rate', 'boat_no', 'boat_within_second_rate', 'boat_within_third_rate', 'pre_time', 'tilt_angle', 'propeller', 'parts', 'adjust_weight', 'pre_start_timing', 'finish_order', 'player_race_time', 'start_timing', 'win_pattern', 'race_grade', 'race_class', 'distance', 'course_direction', 'weather', 'temperature', 'wind', 'wind_direction', 'water_temperature', 'wave_height', 'race_date_old1', 'place_id_old1', 'race_no_old1', 'bracket_no_old1', 'is_miss_old1', 'player_id_old1', 'player_grade_old1', 'branch_old1', 'born_area_old1', 'age_old1', 'weight_old1', 'f_count_old1', 'l_count_old1', 'start_time_avg_old1', 'first_rate_all_old1', 'second_rate_all_old1', 'third_rate_all_old1', 'first_rate_area_old1', 'second_rate_area_old1', 'third_rate_area_old1', 'motor_no_old1', 'motor_within_second_rate_old1', 'motor_within_third_rate_old1', 'boat_no_old1', 'boat_within_second_rate_old1', 'boat_within_third_rate_old1', 'pre_time_old1', 'tilt_angle_old1', 'propeller_old1', 'parts_old1', 'adjust_weight_old1', 'pre_start_timing_old1', 'finish_order_old1', 'player_race_time_old1', 'start_timing_old1', 'win_pattern_old1', 'race_date_old2', 'place_id_old2', 'race_no_old2', 'bracket_no_old2', 'is_miss_old2', 'player_id_old2', 'player_grade_old2', 'branch_old2', 'born_area_old2', 'age_old2', 'weight_old2', 'f_count_old2', 'l_count_old2', 'start_time_avg_old2', 'first_rate_all_old2', 'second_rate_all_old2', 'third_rate_all_old2', 'first_rate_area_old2', 'second_rate_area_old2', 'third_rate_area_old2', 'motor_no_old2', 'motor_within_second_rate_old2', 'motor_within_third_rate_old2', 'boat_no_old2', 'boat_within_second_rate_old2', 'boat_within_third_rate_old2', 'pre_time_old2', 'tilt_angle_old2', 'propeller_old2', 'parts_old2', 'adjust_weight_old2', 'pre_start_timing_old2', 'finish_order_old2', 'player_race_time_old2', 'start_timing_old2', 'win_pattern_old2', 'race_date_old3', 'place_id_old3', 'race_no_old3', 'bracket_no_old3', 'is_miss_old3', 'player_id_old3', 'player_grade_old3', 'branch_old3', 'born_area_old3', 'age_old3', 'weight_old3', 'f_count_old3', 'l_count_old3', 'start_time_avg_old3', 'first_rate_all_old3', 'second_rate_all_old3', 'third_rate_all_old3', 'first_rate_area_old3', 'second_rate_area_old3', 'third_rate_area_old3', 'motor_no_old3', 'motor_within_second_rate_old3', 'motor_within_third_rate_old3', 'boat_no_old3', 'boat_within_second_rate_old3', 'boat_within_third_rate_old3', 'pre_time_old3', 'tilt_angle_old3', 'propeller_old3', 'parts_old3', 'adjust_weight_old3', 'pre_start_timing_old3', 'finish_order_old3', 'player_race_time_old3', 'start_timing_old3', 'win_pattern_old3', 'race_grade_old1','race_class_old1',
                     'distance_old1', 'course_direction_old1', 'weather_old1', 'temperature_old1', 'wind_old1', 'wind_direction_old1', 'water_temperature_old1', 'wave_height_old1', 
                     'race_grade_old2','race_class_old2', 'distance_old2', 'course_direction_old2', 'weather_old2', 'temperature_old2', 'wind_old2', 'wind_direction_old2', 'water_temperature_old2', 'wave_height_old2', 
                     'race_grade_old3','race_class_old3', 'distance_old3', 'course_direction_old3', 'weather_old3', 'temperature_old3', 'wind_old3', 'wind_direction_old3', 'water_temperature_old3', 'wave_height_old3']

PARTS = {'キャリボ': 4,
                 'ピストン': 4,
                 'リング': 3,
                 '電気': 2, 
                 'キャブ': 3,
                 'ギヤ': 2,
                 'シリンダ': 4,
                 'シャフト': 4}

db_settings = {
        "host": '34.85.101.153',
        "database": 'boatrace',
        "user": 'user',
        "password": 'mazikayo',
        "port": '3306',
        "encoding": "utf8",
        "db_name": "maximal-boulder-268803:asia-northeast1:boatrace-mysql"
    }

ex_return = 3000

KANYUSYA_NO = '08342758'
PASS = '2714'
AUTH_PATH = 'Qn7EGt'
BET_PATH = 'nami73'

login_url = 'https://www.boatrace.jp/owpc/pc/login?authAfterUrl=/'

# GCS接続情報
project_name = 'boatrace-ai'
bucket_name = 'boat_race_ai'

class Betting():
    def __init__(self, place_id, race_no, bet_list):
        self.place_id = place_id
        self.race_no = race_no
        self.bet_list = bet_list
        self.bet_amount_sum = 0
        
        #プロジェクト名を指定してclientを作成
        self.client = gcs.Client(project_name)
        #バケット名を指定してbucketを取得
        self.bucket = self.client.get_bucket(bucket_name)
        
    def load_str_from_gcs(self, path):
        blob = self.bucket.get_blob(path)
        return blob.download_as_string()
    
    def load_driver(self):
        # options = webdriver.ChromeOptions()
        # options.add_argument('--headless')
        # self.driver = webdriver.Chrome(options=options)
        self.driver = webdriver.Chrome('asset/chromedriver')
    
    def do_login(self):
        self.driver.get(login_url)
        self.driver.find_element_by_name('in_KanyusyaNo').send_keys(KANYUSYA_NO)
        self.driver.find_element_by_name('in_AnsyoNo').send_keys(PASS)
        self.driver.find_element_by_name('in_PassWord').send_keys(AUTH_PATH)
        time.sleep(0.1)
        self.driver.find_element_by_xpath('/html/body/main/div/div/div/div[2]/div/div/div[2]/div/form/p/button').click()
            
    def open_bet_page(self):
        self.driver.find_element_by_xpath('//*[@id="commonHead"]').click()
        handles = self.driver.window_handles
        time.sleep(0.5)
        self.driver.switch_to_window(handles[1])
        time.sleep(0.2)
        path = '//*[@id="jyo{0}"]/a'.format(self.place_id)
        self.driver.find_element_by_xpath(path).click()
    
    def bet_win(self, bet_inf):
        path = '//*[@id="regbtn_{0}_1"]/a'.format(bet_inf['bracket1'])
        self.driver.find_element_by_xpath(path).click()
        time.sleep(0.1)
        elem = self.driver.find_element_by_xpath('//*[@id="amount"]')
        elem.clear()
        elem.send_keys(bet_inf['amount'])
        time.sleep(0.1)
        self.driver.find_element_by_xpath('//*[@id="regAmountBtn"]/a').click()
        
    def submit(self):
        self.driver.find_element_by_xpath('//*[@id="betList"]/div[3]/div[3]/a').click()
        time.sleep(0.1)
        self.driver.find_element_by_xpath('//*[@id="amount"]').send_keys(self.bet_amount_sum)
        time.sleep(0.1)
        self.driver.find_element_by_xpath('//*[@id="pass"]').send_keys(BET_PATH)
        time.sleep(0.1)
        self.driver.find_element_by_xpath('//*[@id="submitBet"]/a').click()
        time.sleep(0.1)
        self.driver.find_element_by_xpath('//*[@id="ok"]').click()
        
            
    def main_process(self):
        # クロームドライバー読み込み
        self.load_driver()
        
        # ログイン処理
        self.do_login()
        
        # 投票ページに遷移
        self.open_bet_page()
        time.sleep(0.5)
        
        page_state = 0
        
        # 投票作成
        for bet_inf in self.bet_list:
            print(bet_inf)
            
            if bet_inf['bet_type'] == 1:
                if page_state != 1:
                    self.driver.find_element_by_xpath('//*[@id="betkati1"]/a').click()
                    time.sleep(0.1)
                    page_state = 1
                self.bet_win(bet_inf)
                time.sleep(0.1)
            
            self.bet_amount_sum += int(bet_inf['amount'])*100
        
        # 投票確定
        self.submit()

     

class Predict:
    
    def __init__(self, race_date, place_id, race_no):
        self.race_date = race_date
        self.place_id = place_id
        self.race_no = race_no
        
        self.engine = self.load_engine()
        
        #プロジェクト名を指定してclientを作成
        self.client = gcs.Client(project_name)
        #バケット名を指定してbucketを取得
        self.bucket = self.client.get_bucket(bucket_name)
        
    def load_engine(self):
        return create_engine('mysql+pymysql://{user}:{password}@/{database}?unix_socket=/cloudsql/{db_name}'.format(**db_settings))    
        # return create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={encoding}'.format(**db_settings))
        
    def load_str_from_gcs(self, path):
        blob = self.bucket.get_blob(path)
        return blob.download_as_string()
        
    def get_odds_win(self, soup):
        odds = {}
        rows = soup.find_all('table', {'class': 'is-w495'})[0].find_all('tbody')
        for row in rows:
            cell = row.find_all('td')
            if cell[2].get_text() != '欠場':
                odds[cell[0].get_text()] = float(cell[2].get_text())
        return odds

    def get_odds_place(self, soup):
        odds = {}
        rows = soup.find_all('table', {'class': 'is-w495'})[1].find_all('tbody')
        for row in rows:
            cell = row.find_all('td')
            odds[cell[0].get_text()] = [float(cell[2].get_text().split('-')[0]), float(cell[2].get_text().split('-')[1])]
        return odds

    def get_odds_wide(self, soup):
        odds = {}
        
        ar = [s.get_text() for s in soup.find_all('tbody', {'class': 'is-p3-0'})[0].find_all('td',{'class':'oddsPoint'})]
        cmb = []
        a = 1
        b = 2
        for i in range(5):
            for n in range(i+1):
                cmb.append(str(a+n)+'-'+str(b))
            b += 1
        
        for a, c in zip(ar, cmb):
            odds[c] = [float(a.split('-')[0]), float(a.split('-')[1])]
        return odds
        
    def get_odds_triple(self, soup):
        odds = {}
        ar = []
        cell = soup.find_all('td',{'class':'oddsPoint'})

        for i in range(int(len(cell)/6)):
            ar.append([float(c.get_text()) for c in cell[i*6:i*6+6]])
        ar = np.ndarray.flatten(np.array(ar).T).tolist()
        
        cmb = []
        for a in range(6):
            for b in range(6):
                for c in range(6):
                    if (a != b) and (b != c) and (a != c):
                        cmb.append(str(a+1) +'-'+ str(b+1) +'-'+ str(c+1))
        
        for a, c in zip(ar, cmb):
            odds[c] = a
        return odds    
        
    def get_predict_data(self):
        query = """
                select
                rp.race_date,
                rp.place_id,
                rp.race_no,
                rp.race_date_no,
                rp.bracket_no,
                rp.is_miss,
                rp.player_id,
                rp.player_grade,
                rp.branch,
                rp.born_area,
                rp.age,
                rp.weight,
                rp.f_count,
                rp.l_count,
                rp.start_time_avg,
                rp.first_rate_all,
                rp.second_rate_all,
                rp.third_rate_all,
                rp.first_rate_area,
                rp.second_rate_area,
                rp.third_rate_area,
                rp.motor_no,
                rp.motor_within_second_rate,
                rp.motor_within_third_rate,
                rp.boat_no,
                rp.boat_within_second_rate,
                rp.boat_within_third_rate,
                rp.pre_time,
                rp.tilt_angle,
                rp.propeller,
                rp.parts,
                rp.adjust_weight,
                rp.pre_start_timing,
                rp.finish_order,
                rp.player_race_time,
                rp.start_timing,
                rp.win_pattern,
                rd.race_grade,
                rd.race_class,
                rd.distance,
                rd.course_direction,
                rd.weather,
                rd.temperature,
                rd.wind,
                rd.wind_direction,
                rd.water_temperature,
                rd.wave_height,
                rp1.race_date as race_date_old1,
                rp1.place_id as place_id_old1,
                rp1.race_no as race_no_old1,
                rp1.bracket_no as bracket_no_old1,
                rp1.is_miss as is_miss_old1,
                rp1.player_id as player_id_old1,
                rp1.player_grade as player_grade_old1,
                rp1.branch as branch_old1,
                rp1.born_area as born_area_old1,
                rp1.age as age_old1,
                rp1.weight as weight_old1,
                rp1.f_count as f_count_old1,
                rp1.l_count as l_count_old1,
                rp1.start_time_avg as start_time_avg_old1,
                rp1.first_rate_all as first_rate_all_old1,
                rp1.second_rate_all as second_rate_all_old1,
                rp1.third_rate_all as third_rate_all_old1,
                rp1.first_rate_area as first_rate_area_old1,
                rp1.second_rate_area as second_rate_area_old1,
                rp1.third_rate_area as third_rate_area_old1,
                rp1.motor_no as motor_no_old1,
                rp1.motor_within_second_rate as motor_within_second_rate_old1,
                rp1.motor_within_third_rate as motor_within_third_rate_old1,
                rp1.boat_no as boat_no_old1,
                rp1.boat_within_second_rate as boat_within_second_rate_old1,
                rp1.boat_within_third_rate as boat_within_third_rate_old1,
                rp1.pre_time as pre_time_old1,
                rp1.tilt_angle as tilt_angle_old1,
                rp1.propeller as propeller_old1,
                rp1.parts as parts_old1,
                rp1.adjust_weight as adjust_weight_old1,
                rp1.pre_start_timing as pre_start_timing_old1,
                rp1.finish_order as finish_order_old1,
                rp1.player_race_time as player_race_time_old1,
                rp1.start_timing as start_timing_old1,
                rp1.win_pattern as win_pattern_old1,
                rp2.race_date as race_date_old2,
                rp2.place_id as place_id_old2,
                rp2.race_no as race_no_old2,
                rp2.bracket_no as bracket_no_old2,
                rp2.is_miss as is_miss_old2,
                rp2.player_id as player_id_old2,
                rp2.player_grade as player_grade_old2,
                rp2.branch as branch_old2,
                rp2.born_area as born_area_old2,
                rp2.age as age_old2,
                rp2.weight as weight_old2,
                rp2.f_count as f_count_old2,
                rp2.l_count as l_count_old2,
                rp2.start_time_avg as start_time_avg_old2,
                rp2.first_rate_all as first_rate_all_old2,
                rp2.second_rate_all as second_rate_all_old2,
                rp2.third_rate_all as third_rate_all_old2,
                rp2.first_rate_area as first_rate_area_old2,
                rp2.second_rate_area as second_rate_area_old2,
                rp2.third_rate_area as third_rate_area_old2,
                rp2.motor_no as motor_no_old2,
                rp2.motor_within_second_rate as motor_within_second_rate_old2,
                rp2.motor_within_third_rate as motor_within_third_rate_old2,
                rp2.boat_no as boat_no_old2,
                rp2.boat_within_second_rate as boat_within_second_rate_old2,
                rp2.boat_within_third_rate as boat_within_third_rate_old2,
                rp2.pre_time as pre_time_old2,
                rp2.tilt_angle as tilt_angle_old2,
                rp2.propeller as propeller_old2,
                rp2.parts as parts_old2,
                rp2.adjust_weight as adjust_weight_old2,
                rp2.pre_start_timing as pre_start_timing_old2,
                rp2.finish_order as finish_order_old2,
                rp2.player_race_time as player_race_time_old2,
                rp2.start_timing as start_timing_old2,
                rp2.win_pattern as win_pattern_old2,
                rp3.race_date as race_date_old3,
                rp3.place_id as place_id_old3,
                rp3.race_no as race_no_old3,
                rp3.bracket_no as bracket_no_old3,
                rp3.is_miss as is_miss_old3,
                rp3.player_id as player_id_old3,
                rp3.player_grade as player_grade_old3,
                rp3.branch as branch_old3,
                rp3.born_area as born_area_old3,
                rp3.age as age_old3,
                rp3.weight as weight_old3,
                rp3.f_count as f_count_old3,
                rp3.l_count as l_count_old3,
                rp3.start_time_avg as start_time_avg_old3,
                rp3.first_rate_all as first_rate_all_old3,
                rp3.second_rate_all as second_rate_all_old3,
                rp3.third_rate_all as third_rate_all_old3,
                rp3.first_rate_area as first_rate_area_old3,
                rp3.second_rate_area as second_rate_area_old3,
                rp3.third_rate_area as third_rate_area_old3,
                rp3.motor_no as motor_no_old3,
                rp3.motor_within_second_rate as motor_within_second_rate_old3,
                rp3.motor_within_third_rate as motor_within_third_rate_old3,
                rp3.boat_no as boat_no_old3,
                rp3.boat_within_second_rate as boat_within_second_rate_old3,
                rp3.boat_within_third_rate as boat_within_third_rate_old3,
                rp3.pre_time as pre_time_old3,
                rp3.tilt_angle as tilt_angle_old3,
                rp3.propeller as propeller_old3,
                rp3.parts as parts_old3,
                rp3.adjust_weight as adjust_weight_old3,
                rp3.pre_start_timing as pre_start_timing_old3,
                rp3.finish_order as finish_order_old3,
                rp3.player_race_time as player_race_time_old3,
                rp3.start_timing as start_timing_old3,
                rp3.win_pattern as win_pattern_old3,
                rd1.race_grade as race_grade_old1,
                rd1.race_class as race_class_old1,
                rd1.distance as distance_old1,
                rd1.course_direction as course_direction_old1,
                rd1.weather as weather_old1,
                rd1.temperature as temperature_old1,
                rd1.wind as wind_old1,
                rd1.wind_direction as wind_direction_old1,
                rd1.water_temperature as water_temperature_old1,
                rd1.wave_height as wave_height_old1,
                rd2.race_grade as race_grade_old2,
                rd2.race_class as race_class_old2,
                rd2.distance as distance_old2,
                rd2.course_direction as course_direction_old2,
                rd2.weather as weather_old2,
                rd2.temperature as temperature_old2,
                rd2.wind as wind_old2,
                rd2.wind_direction as wind_direction_old2,
                rd2.water_temperature as water_temperature_old2,
                rd2.wave_height as wave_height_old2,
                rd3.race_grade as race_grade_old3,
                rd3.race_class as race_class_old3,
                rd3.distance as distance_old3,
                rd3.course_direction as course_direction_old3,
                rd3.weather as weather_old3,
                rd3.temperature as temperature_old3,
                rd3.wind as wind_old3,
                rd3.wind_direction as wind_direction_old3,
                rd3.water_temperature as water_temperature_old3,
                rd3.wave_height as wave_height_old3

            from (select * from race_player 
                    where race_date = {race_date}
                    and place_id = {place_id}
                    and race_no = {race_no}
                    and is_miss = 'False'
                    order by bracket_no
                    ) rp
            inner join race_detail rd
                on rd.race_date = rp.race_date
                and rd.place_id = rp.place_id
                and rd.race_no = rp.race_no

            left join race_player rp1
                on rp.player_id = rp1.player_id
                and rp1.race_date_no = (select race_date_no from race_player rpt1
                                            where rpt1.race_date_no < rp.race_date_no
                                                and rp.player_id = rpt1.player_id
                                            order by rpt1.race_date_no desc limit 1
                                        )

            left join race_player rp2
                on rp.player_id = rp2.player_id
                and rp2.race_date_no = (select race_date_no from race_player rpt2
                                            where rpt2.race_date_no < rp1.race_date_no
                                                and rp.player_id = rpt2.player_id
                                            order by rpt2.race_date_no desc limit 1
                                        )

            left join race_player rp3
                on rp.player_id = rp3.player_id
                and rp3.race_date_no = (select race_date_no from race_player rpt3
                                            where rpt3.race_date_no < rp2.race_date_no
                                                and rp.player_id = rpt3.player_id
                                            order by rpt3.race_date_no desc limit 1
                                        )
            left join race_detail rd1
                on rd1.race_date = rp1.race_date
                and rd1.place_id = rp1.place_id
                and rd1.race_no = rp1.race_no

            left join race_detail rd2
                on rd2.race_date = rp2.race_date
                and rd2.place_id = rp2.place_id
                and rd2.race_no = rp2.race_no

            left join race_detail rd3
                on rd3.race_date = rp3.race_date
                and rd3.place_id = rp3.place_id
                and rd3.race_no = rp3.race_no
        """

        race_inf = {
                    'race_date': self.race_date,
                    'place_id': self.place_id,
                    'race_no': self.race_no
                }
        
        query = query.format(**race_inf).replace('\n', ' ')
        
        #クエリ実行
        res = self.engine.execute(query)
        
        return [r for r in res]
    
    def parts_count(self, x, key, num):
        x = str(x)
        if x.find(key) != -1:
            if x[x.find(key):].find('×') != -1:
                return x[x.find(key)+num+1]
            else:
                return 1
        else:
            return 0
        
    def conv_time(self, x):
        if str(x).find('nan') != -1:
            return x
        elif str(x).find(':') == -1:
            return np.nan
        else:
            time = x.split(':')
            return float(time[0])*60 + float(time[1]) + float(time[2])/10   
    
    def preprocessing(self):
        # 月
        self.df['month'] = self.df['race_date'].astype(int).apply(lambda x: (x%10000)//100)
        self.df['month_old1'] = self.df['race_date_old1'].astype(int).apply(lambda x: (x%10000)//100)
        self.df['month_old2'] = self.df['race_date_old2'].astype(int).apply(lambda x: (x%10000)//100)
        self.df['month_old3'] = self.df['race_date_old3'].astype(int).apply(lambda x: (x%10000)//100)
        # 日
        self.df['date'] = self.df['race_date'].astype(int).apply(lambda x: x%100)
        self.df['date_old1'] = self.df['race_date_old1'].astype(int).apply(lambda x: x%100)
        self.df['date_old2'] = self.df['race_date_old2'].astype(int).apply(lambda x: x%100)
        self.df['date_old3'] = self.df['race_date_old3'].astype(int).apply(lambda x: x%100)
        
        # 周期データを三角関数に変換
        # 月
        self.df['month'] = self.df['month'].astype(int)
        self.df['month_old1'] = self.df['month_old1'].astype(int)
        self.df['month_old2'] = self.df['month_old2'].astype(int)
        self.df['month_old3'] = self.df['month_old3'].astype(int)
        
        self.df['date'] = self.df['date'].astype(int)
        self.df['date_old1'] = self.df['date_old1'].astype(int)
        self.df['date_old2'] = self.df['date_old2'].astype(int)
        self.df['date_old3'] = self.df['date_old3'].astype(int)
        
        self.df['course_direction'] = self.df['course_direction'].astype(int)
        self.df['course_direction_old1'] = self.df['course_direction_old1'].astype(int)
        self.df['course_direction_old2'] = self.df['course_direction_old2'].astype(int)
        self.df['course_direction_old3'] = self.df['course_direction_old3'].astype(int)
        
        self.df['wind_direction'] = self.df['wind_direction'].astype(int)
        self.df['wind_direction_old1'] = self.df['wind_direction_old1'].astype(int)
        self.df['wind_direction_old2'] = self.df['wind_direction_old2'].astype(int)
        self.df['wind_direction_old3'] = self.df['wind_direction_old3'].astype(int)
        
        
        self.df['month_cos'] = np.cos(2 * np.pi * self.df['month']/self.df['month'].max())
        self.df['month_sin'] = np.sin(2 * np.pi * self.df['month']/self.df['month'].max())
        self.df['month_cos_old1'] = np.cos(2 * np.pi * self.df['month_old1']/self.df['month_old1'].max())
        self.df['month_sin_old1'] = np.sin(2 * np.pi * self.df['month_old1']/self.df['month_old1'].max())
        self.df['month_cos_old2'] = np.cos(2 * np.pi * self.df['month_old2']/self.df['month_old2'].max())
        self.df['month_sin_old2'] = np.sin(2 * np.pi * self.df['month_old2']/self.df['month_old2'].max())
        self.df['month_cos_old3'] = np.cos(2 * np.pi * self.df['month_old3']/self.df['month_old3'].max())
        self.df['month_sin_old3'] = np.sin(2 * np.pi * self.df['month_old3']/self.df['month_old3'].max())
        # 日
        self.df['date_cos'] = np.cos(2 * np.pi * self.df['date']/self.df['date'].max())
        self.df['date_sin'] = np.sin(2 * np.pi * self.df['date']/self.df['date'].max())
        self.df['date_cos_old1'] = np.cos(2 * np.pi * self.df['date_old1']/self.df['date_old1'].max())
        self.df['date_sin_old1'] = np.sin(2 * np.pi * self.df['date_old1']/self.df['date_old1'].max())
        self.df['date_cos_old2'] = np.cos(2 * np.pi * self.df['date_old2']/self.df['date_old2'].max())
        self.df['date_sin_old2'] = np.sin(2 * np.pi * self.df['date_old2']/self.df['date_old2'].max())
        self.df['date_cos_old3'] = np.cos(2 * np.pi * self.df['date_old3']/self.df['date_old3'].max())
        self.df['date_sin_old3'] = np.sin(2 * np.pi * self.df['date_old3']/self.df['date_old3'].max())
        # コース方向
        self.df['course_direction_cos'] = np.cos(2 * np.pi * self.df['course_direction']/self.df['course_direction'].max())
        self.df['course_direction_sin'] = np.sin(2 * np.pi * self.df['course_direction']/self.df['course_direction'].max())
        self.df['course_direction_cos_old1'] = np.cos(2 * np.pi * self.df['course_direction_old1']/self.df['course_direction_old1'].max())
        self.df['course_direction_sin_old1'] = np.sin(2 * np.pi * self.df['course_direction_old1']/self.df['course_direction_old1'].max())
        self.df['course_direction_cos_old2'] = np.cos(2 * np.pi * self.df['course_direction_old2']/self.df['course_direction_old2'].max())
        self.df['course_direction_sin_old2'] = np.sin(2 * np.pi * self.df['course_direction_old2']/self.df['course_direction_old2'].max())
        self.df['course_direction_cos_old3'] = np.cos(2 * np.pi * self.df['course_direction_old3']/self.df['course_direction_old3'].max())
        self.df['course_direction_sin_old3'] = np.sin(2 * np.pi * self.df['course_direction_old3']/self.df['course_direction_old3'].max())
        # 風方向
        self.df['wind_direction_cos'] = np.cos(2 * np.pi * self.df['wind_direction']/self.df['wind_direction'].max())
        self.df['wind_direction_sin'] = np.sin(2 * np.pi * self.df['wind_direction']/self.df['wind_direction'].max())
        self.df['wind_direction_cos_old1'] = np.cos(2 * np.pi * self.df['wind_direction_old1']/self.df['wind_direction_old1'].max())
        self.df['wind_direction_sin_old1'] = np.sin(2 * np.pi * self.df['wind_direction_old1']/self.df['wind_direction_old1'].max())
        self.df['wind_direction_cos_old2'] = np.cos(2 * np.pi * self.df['wind_direction_old2']/self.df['wind_direction_old2'].max())
        self.df['wind_direction_sin_old2'] = np.sin(2 * np.pi * self.df['wind_direction_old2']/self.df['wind_direction_old2'].max())
        self.df['wind_direction_cos_old3'] = np.cos(2 * np.pi * self.df['wind_direction_old3']/self.df['wind_direction_old3'].max())
        self.df['wind_direction_sin_old3'] = np.sin(2 * np.pi * self.df['wind_direction_old3']/self.df['wind_direction_old3'].max())
        # 風とコースの差分方向
        self.df['course_wind_direction_cos'] = np.cos(2 * np.pi * (self.df['wind_direction']/self.df['wind_direction'].max() - self.df['course_direction']/self.df['course_direction'].max()))
        self.df['course_wind_direction_sin'] = np.sin(2 * np.pi * (self.df['wind_direction']/self.df['wind_direction'].max() - self.df['course_direction']/self.df['course_direction'].max()))
        self.df['course_wind_direction_cos_old1'] = np.cos(2 * np.pi * (self.df['wind_direction_old1']/self.df['wind_direction_old1'].max() - self.df['course_direction_old1']/self.df['course_direction_old1'].max()))
        self.df['course_wind_direction_sin_old1'] = np.sin(2 * np.pi * (self.df['wind_direction_old1']/self.df['wind_direction_old1'].max() - self.df['course_direction_old1']/self.df['course_direction_old1'].max()))
        self.df['course_wind_direction_cos_old2'] = np.cos(2 * np.pi * (self.df['wind_direction_old2']/self.df['wind_direction_old2'].max() - self.df['course_direction_old2']/self.df['course_direction_old2'].max()))
        self.df['course_wind_direction_sin_old2'] = np.sin(2 * np.pi * (self.df['wind_direction_old2']/self.df['wind_direction_old2'].max() - self.df['course_direction_old2']/self.df['course_direction_old2'].max()))
        self.df['course_wind_direction_cos_old3'] = np.cos(2 * np.pi * (self.df['wind_direction_old3']/self.df['wind_direction_old3'].max() - self.df['course_direction_old3']/self.df['course_direction_old3'].max()))
        self.df['course_wind_direction_sin_old3'] = np.sin(2 * np.pi * (self.df['wind_direction_old3']/self.df['wind_direction_old3'].max() - self.df['course_direction_old3']/self.df['course_direction_old3'].max()))
        
        # 展示フライングフラグ
        self.df['pre_f_flg'] = self.df['pre_start_timing'].apply(lambda x: str(x).find('F')!=-1)
        self.df['pre_l_flg'] = self.df['pre_start_timing'].apply(lambda x: str(x).find('L')!=-1)
        self.df['pre_start_timing'] = self.df['pre_start_timing'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        self.df['pre_f_flg_old1'] = self.df['pre_start_timing_old1'].apply(lambda x: str(x).find('F')!=-1)
        self.df['pre_l_flg_old1'] = self.df['pre_start_timing_old1'].apply(lambda x: str(x).find('L')!=-1)
        self.df['pre_start_timing_old1'] = self.df['pre_start_timing_old1'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        self.df['start_timing_old1'] = self.df['start_timing_old1'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        self.df['pre_f_flg_old2'] = self.df['pre_start_timing_old2'].apply(lambda x: str(x).find('F')!=-1)
        self.df['pre_l_flg_old2'] = self.df['pre_start_timing_old2'].apply(lambda x: str(x).find('L')!=-1)
        self.df['pre_start_timing_old2'] = self.df['pre_start_timing_old2'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        self.df['start_timing_old2'] = self.df['start_timing_old2'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        self.df['pre_f_flg_old3'] = self.df['pre_start_timing_old3'].apply(lambda x: str(x).find('F')!=-1)
        self.df['pre_l_flg_old3'] = self.df['pre_start_timing_old3'].apply(lambda x: str(x).find('L')!=-1)
        self.df['pre_start_timing_old3'] = self.df['pre_start_timing_old3'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        self.df['start_timing_old3'] = self.df['start_timing_old3'].apply(lambda x: -1*float(x.replace('F', '0')) if str(x).find('F')!=-1 else (np.nan if str(x).find('L')!=-1 else float(x)))
        
        # パーツフラグと個数
        for key, values in PARTS.items():
            self.df[key+'_flg'] = self.df['parts'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count'] = self.df['parts'].apply(self.parts_count, key=key, num=PARTS[key])
            self.df[key+'_flg_old1'] = self.df['parts_old1'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count_old1'] = self.df['parts_old1'].apply(self.parts_count, key=key, num=PARTS[key])
            self.df[key+'_flg_old2'] = self.df['parts_old2'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count_old2'] = self.df['parts_old2'].apply(self.parts_count, key=key, num=PARTS[key])
            self.df[key+'_flg_old3'] = self.df['parts_old3'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count_old3'] = self.df['parts_old3'].apply(self.parts_count, key=key, num=PARTS[key])
            
        # 枠番
        self.df['bracket_num'] = self.df['bracket_no']
        self.df['bracket_num_old1'] = self.df['bracket_no_old1']
        self.df['bracket_num_old2'] = self.df['bracket_no_old2']
        self.df['bracket_num_old3'] = self.df['bracket_no_old3']
        
        # 過去レースタイム変換
        self.df['player_race_time_old1'] = self.df['player_race_time_old1'].apply(self.conv_time)
        self.df['player_race_time_old2'] = self.df['player_race_time_old2'].apply(self.conv_time)
        self.df['player_race_time_old3'] = self.df['player_race_time_old3'].apply(self.conv_time)
        
        # 特選フラグ
        # TODO
        
        # 前走からの日付
        #コンドツクル
        
        # ターゲットエンコーディング
        group = self.df.groupby(['bracket_no'])
        # 枠順ごとの着順平均
        bracket_order_mean = pd.read_csv(BytesIO(self.load_str_from_gcs('csv/bracket_order_mean.csv')))
        bracket_order_mean['bracket_no'] = bracket_order_mean.index+1
        bracket_order_mean['bracket_no'] = bracket_order_mean['bracket_no'].astype('str')
        self.df = pd.merge(self.df, bracket_order_mean, on='bracket_no')
        # 枠順ごとの着順中央値
        bracket_order_median = pd.read_csv(BytesIO(self.load_str_from_gcs('csv/bracket_order_median.csv')))
        bracket_order_median['bracket_no'] = bracket_order_median.index+1
        bracket_order_median['bracket_no'] = bracket_order_median['bracket_no'].astype('str')
        self.df = pd.merge(self.df, bracket_order_median, on='bracket_no')
        
        self.df = self.df.sort_values(by=['race_date', 'place_id', 'race_no', 'bracket_no'])
        
        # n走前nullフラグ作成
        self.df['race_date_old1'] = self.df['race_date_old1'].astype('float')
        self.df['race_date_old2'] = self.df['race_date_old2'].astype('float')
        self.df['race_date_old3'] = self.df['race_date_old3'].astype('float')
        
        self.df['old1_is_null'] = self.df['race_date_old1'].apply(lambda x: np.isnan(x))
        self.df['old2_is_null'] = self.df['race_date_old2'].apply(lambda x: np.isnan(x))
        self.df['old3_is_null'] = self.df['race_date_old3'].apply(lambda x: np.isnan(x))
        
        # レース内統計
        group = self.df.groupby(['race_date', 'place_id', 'race_no'])
        stat_col = [
                'age', 'weight', 'f_count',
                'l_count', 'start_time_avg', 'first_rate_all', 'second_rate_all',
                'third_rate_all', 'first_rate_area', 'second_rate_area',
                'third_rate_area', 'motor_within_second_rate',
                'motor_within_third_rate', 'boat_within_second_rate',
                'boat_within_third_rate', 'pre_time', 'tilt_angle', 'adjust_weight'
                ]
        name_mean = {}
        name_std = {}
        name_max = {}
        name_min = {}
        name_rank = {}
        for c in stat_col:
            name_mean[c] = c + '_mean'
            name_std[c] = c + '_std'
            name_max[c] = c + '_max'
            name_min[c] = c + '_min'
        
        mean_data = group[stat_col].mean()
        std_data = group[stat_col].std()
        max_data = group[stat_col].max()
        min_data = group[stat_col].min()
        
        mean_data = mean_data.rename(columns = name_mean)
        std_data = std_data.rename(columns = name_std)
        max_data = max_data.rename(columns = name_max)
        min_data = min_data.rename(columns = name_min)
        
        self.df = pd.merge(self.df, mean_data, on = ['race_date', 'place_id','race_no'])
        self.df = pd.merge(self.df, std_data, on = ['race_date', 'place_id','race_no'])
        self.df = pd.merge(self.df, max_data, on = ['race_date', 'place_id','race_no'])
        self.df = pd.merge(self.df, min_data, on = ['race_date', 'place_id','race_no'])
        
        # ランキング
        group = self.df.groupby(['race_date', 'place_id', 'race_no'])
        rank_col = [
                'age', 'weight', 'start_time_avg', 'first_rate_all', 'second_rate_all',
                'third_rate_all', 'first_rate_area', 'second_rate_area',
                'third_rate_area', 'motor_within_second_rate',
                'motor_within_third_rate', 'boat_within_second_rate',
                'boat_within_third_rate', 'pre_time'
                ]
        for c in stat_col:
            name_rank[c] = c + '_rank'

        rank_data = group[rank_col].rank(ascending=True,method="first")
        rank_data = rank_data.rename(columns = name_rank)
        self.df = pd.concat([self.df, rank_data], axis = 1)
        
        #One-Hot Encoding
        dummies_list = ['place_id','bracket_no','player_grade','branch','born_area','propeller','weather','race_grade']
        past_dummies_list = ['place_id','bracket_no','player_grade','branch','born_area','propeller','weather','win_pattern','race_grade']
        
        for c in past_dummies_list:
            for i in range(3):
                dummies_list.append(c+'_old'+str(i+1))
        for i in range(3):
            dummies_list.append('old'+str(i+1)+'_is_null')
        for key in PARTS.keys():
            dummies_list.append(key+'_flg')
            
        self.df = pd.get_dummies(data = self.df, columns = dummies_list)
        
        
        # 不要カラムの削除
        drop_list = ['race_date','race_date_no','is_miss','player_id','parts','parts_old1','parts_old2','parts_old3',
                            'motor_no','boat_no','finish_order','player_race_time',
                            'start_timing','win_pattern','race_date_old1',
                            'is_miss_old1','player_id_old1','motor_no_old1',
                            'boat_no_old1','race_date_old2','is_miss_old2',
                            'player_id_old2','motor_no_old2','boat_no_old2',
                            'race_date_old3','is_miss_old3','player_id_old3',
                            'motor_no_old3','boat_no_old3']
        
        drop_list.extend(['race_class', 'race_class_old1', 'race_class_old2', 'race_class_old3'])
        
        input_shape = pd.read_csv(BytesIO(self.load_str_from_gcs('csv/input_shape.csv')))
        add_columns = list(filter(lambda x: x not in self.df.columns, input_shape.columns))
        
        self.df = pd.concat([self.df,input_shape])[input_shape.columns]
        
        for c in add_columns:
            self.df[c] = self.df[c].fillna(0)
        
    
    def weight_to_int(self):
        self.df['weight'] = self.df['weight'].apply(lambda x: math.floor(float(x)))
        self.df['weight_old1'] = self.df['weight_old1'].apply(lambda x: math.floor(float(x)))
        self.df['weight_old2'] = self.df['weight_old2'].apply(lambda x: math.floor(float(x)))
        self.df['weight_old3'] = self.df['weight_old3'].apply(lambda x: math.floor(float(x)))
            
    def load_model(self):
        self.model = lgb.Booster(model_str=self.load_str_from_gcs('models/first_model_target.txt').decode())
        
    def get_pred(self):
        output = self.model.predict(self.df.values)
        return output[:,1]/sum(output[:,1])

    def get_expected_value(self):
        url = 'https://www.boatrace.jp/owpc/pc/race/oddstf?rno={race_no}&jcd={place_no}&hd={race_date}'
        url = url.format(**{'race_date': self.race_date ,'place_no': "{0:02d}".format(int(self.place_id)), 'race_no': self.race_no})
        html = urlopen(url)
        soup = BeautifulSoup(html,"html.parser")
        self.odds_win = self.get_odds_win(soup)
        
        self.ex_value = {}
        for pred, odds in zip(self.output, self.odds_win.items()):
            self.ex_value[odds[0]] = pred*odds[1]
            
    def bet(self):
        bet_list = []
        
        for pred, ex in zip(self.output, self.ex_value.items()):
            if ex[1] > 1 and pred > 0.02:
                1 if ex_return/(self.odds_win[ex[0]]*100) == 0 else ex_return/(self.odds_win[ex[0]]*100)
                
                bet = {
                    'bet_type': 1 ,
                    'bracket1': ex[0],
                    'bracket2': None,
                    'bracket3': None,
                    'amount' : 1 if ex_return/(self.odds_win[ex[0]]*100) == 0 else ex_return/(self.odds_win[ex[0]]*100)
                    }
                print(bet)
                bet_list.append(bet)
        
        # betting = Betting(self.place_id, self.race_no, bet_list)
        # betting.main_process()
        
    def insert_win(self):
        for i, pred in enumerate(self.output):
        
            query = """
                    insert into predict_result (race_date, place_id, race_no, bet_type, sub_number, bracket_no_1, pred, created_time)
                    value({race_date}, {place_id}, {race_no}, {bet_type}, {sub_number}, {bracket_no_1}, {pred}, now())
                    """
            data = {
                'race_date': self.race_date,
                'place_id': self.place_id,
                'race_no': self.race_no,
                'bet_type': 1,
                'sub_number': i,
                'bracket_no_1': str(i+1),
                'pred': pred
            }
            self.engine.execute(query.format(**data))

    
    def main_proess(self):
        
        # 予測用データ取得
        self.df = pd.DataFrame(self.get_predict_data(), columns=LOAD_DATA_COLUMNS)
        
        # データ前処理
        self.preprocessing()
        
        # モデル読み込み
        self.load_model()
        
        # 確率出力
        self.output = self.get_pred()

        # 期待値計算
        self.get_expected_value()

        print(self.output)
        print(self.ex_value)
        
        self.insert_win()
        
        self.bet()
        
        return self.ex_value

def main(request):
    request_json = request.get_json()
    date = request_json['race_date']
    place_id = request_json['place_id']
    race_no = request_json['race_no']
    
    predict = Predict(date, place_id, race_no)
    
    predict.main_proess()