import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from selenium import webdriver
import sys
import time
import traceback

KANYUSYA_NO = '08342758'
PASS = '2714'
AUTH_PATH = 'Qn7EGt'
BET_PATH = 'nami73'

login_url = 'https://www.boatrace.jp/owpc/pc/login?authAfterUrl=/'

# bet_list_sample
# [{
# 'bet_type': ,
# 'bracket1': ,
# 'bracket2': ,
# 'bracket3': ,
# 'amount': 
# },,,,]

# TODO
# 投票結果をDB登録しましょう

class Betting():
    def __init__(self, place_id, race_no, bet_list):
        self.place_id = place_id
        self.race_no = race_no
        self.bet_list = bet_list
        self.bet_amount_sum = 0
    
    def load_driver(self):
        self.driver = webdriver.Chrome('./chromedriver')
    
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
        try:
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
        except:
            traceback.print_exc()
        
    
if __name__ == '__main__':
    betting = Betting('24', '10', [{
    'bet_type': 1 ,
    'bracket1': '1',
    'bracket2': None,
    'bracket3': None,
    'amount' : '5'
    }, 
    {
    'bet_type': 1 ,
    'bracket1': '2',
    'bracket2': None,
    'bracket3': None,
    'amount' : '3'
    },                             
    ])
    betting.main_process()