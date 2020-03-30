import requests
import datetime
import sys
import traceback
import logging
import json
import threading
import time
from selenium import webdriver
from requests_oauthlib import OAuth1Session

palce_code_master = {
    '1': '桐生',
    '2': '戸田',
    '3': '江戸川',
    '4': '平和島',
    '5': '多摩川',
    '6': '浜名湖',
    '7': '蒲郡',
    '8': '常滑',
    '9': '津',
    '10': '三国',
    '11': 'びわこ',
    '12': '住之江',
    '13': '尼崎',
    '14': '鳴門',
    '15': '丸亀',
    '16': '児島',
    '17': '宮島',
    '18': '徳山',
    '19': '下関',
    '20': '若松',
    '21': '芦屋',
    '22': '福岡',
    '23': '唐津',
    '24': '大村',
    }

get_future_racetime_api_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/get_future_racetime'
insert_race_data_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/insert_race_data'
predict_race_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/predict_race'
get_expected_value_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/get_expected_value'
update_race_date_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/update_race_data'

ex_return = 2000
ex_th = 1

KANYUSYA_NO = '08342758'
PASS = '2714'
AUTH_PATH = 'Qn7EGt'
BET_PATH = 'nami73'

login_url = 'https://www.boatrace.jp/owpc/pc/login?authAfterUrl=/'

CK = '57TqATZ3YVjIFoQRSB6CtJ7cJ'
CS = 'z3dtAIOborX0R2kr8PlUOuVvqKkmDx06cSnI77zbsih1C4ABjk'
AT = '1242725416575492097-CPCef8OpD8J11kBCriaU3Bxl54XHdn'
ATS = 'fKD1PEC7Oy5o1XdY9fR0nA2sNhgMY2RbD3foBjDty4shO'

twitter = OAuth1Session(CK, CS, AT, ATS)


class Betting():
    def __init__(self, place_id, race_no,):
        self.place_id = place_id
        self.race_no = race_no
        # self.bet_list = bet_list
        self.bet_amount_sum = 0
    
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
        self.handles = self.driver.window_handles
        time.sleep(60)
        self.driver.switch_to_window(self.handles[1])
        time.sleep(15)
        path = '//*[@id="jyo{0}"]/a'.format("{0:02d}".format(int(self.place_id)))
        self.driver.find_element_by_xpath(path).click()
    
    def bet_win(self, bet_inf):
        path = '//*[@id="regbtn_{0}_1"]/a'.format(bet_inf['bracket1'])
        self.driver.find_element_by_xpath(path).click()
        time.sleep(0.3)
        elem = self.driver.find_element_by_xpath('//*[@id="amount"]')
        elem.clear()
        elem.send_keys(bet_inf['amount'])
        time.sleep(0.3)
        self.driver.find_element_by_xpath('//*[@id="regAmountBtn"]/a').click()
        
    def submit(self):
        self.driver.find_element_by_xpath('//*[@id="betList"]/div[3]/div[3]/a').click()
        time.sleep(0.3)
        self.driver.find_element_by_xpath('//*[@id="amount"]').send_keys(self.bet_amount_sum)
        time.sleep(0.3)
        self.driver.find_element_by_xpath('//*[@id="pass"]').send_keys(BET_PATH)
        time.sleep(0.3)
        self.driver.find_element_by_xpath('//*[@id="submitBet"]/a').click()
        time.sleep(0.3)
        self.driver.find_element_by_xpath('//*[@id="ok"]').click()
        
    def open_betting_page(self):
        # クロームドライバー読み込み
        self.load_driver()
        
        # ログイン処理
        self.do_login()
        
        # 投票ページに遷移
        self.open_bet_page()
        time.sleep(1)
        
    def do_bet(self, bet_list):
        page_state = 0
        
        # 投票作成
        for bet_inf in bet_list:
            print(bet_inf)
            
            if bet_inf['bet_type'] == 1:
                if page_state != 1:
                    self.driver.find_element_by_xpath('//*[@id="betkati1"]/a').click()
                    time.sleep(1)
                    page_state = 1
                self.bet_win(bet_inf)
                time.sleep(1)
            
            self.bet_amount_sum += int(bet_inf['amount'])*100
        
        # 投票確定
        self.submit()

        self.driver.close()
        self.driver.switch_to_window(self.handles[0])
        self.driver.close()
        
            
    def main_process(self):
        # クロームドライバー読み込み
        self.load_driver()
        
        # ログイン処理
        self.do_login()
        
        # 投票ページに遷移
        self.open_bet_page()
        time.sleep(1)
        
        page_state = 0
        
        # 投票作成
        for bet_inf in self.bet_list:
            print(bet_inf)
            
            if bet_inf['bet_type'] == 1:
                if page_state != 1:
                    self.driver.find_element_by_xpath('//*[@id="betkati1"]/a').click()
                    time.sleep(1)
                    page_state = 1
                self.bet_win(bet_inf)
                time.sleep(1)
            
            self.bet_amount_sum += int(bet_inf['amount'])*100
        
        # 投票確定
        self.submit()

        self.driver.close()

def get_racetime_list():
    res = requests.post(
                        get_future_racetime_api_url ,
                        json={}
                        )
    
    text = res.text
    race_list = text.split('-,-')
    
    racetime_list = []
    for race in race_list:
        racetime_list.append(json.loads(race))
    return racetime_list

def tweet_result(race_date, place_id, race_no, bet_list):
    url = "https://api.twitter.com/1.1/statuses/update.json"
    txt = "{race_date} {place_id} {race_no}R\n".format(race_date=race_date, place_id=palce_code_master[place_id], race_no=race_no)
    for bet in bet_list:
        txt += "単勝 {bracket1} ¥{amount}00\n".format(**bet)
        
    params = {"status" : txt}
    
    req = twitter.post(url, params = params)

    if req.status_code == 200:
        print("Succeed Tweet")
    else:
        print("ERROR : %d"% req.status_code)

def thred_tsk(race_date, place_id, race_no, vote_deadline):
    body = {"race_date":race_date,
            "place_id": "{0:02d}".format(int(place_id)),
            "race_no": race_no
            }
    for i in range(10):
        res = requests.post(
            insert_race_data_url,
            json=body
        )
        if res.status_code==200:
            break
        if i == 9:
            print('Faild: insert_race_date')
            return
        time.sleep(10) 
        
    print('Success: insert_race_date')
    
    for i in range(10):
        res = requests.post(
            predict_race_url,
            json=body)
        if res.status_code==200:
            break
        if i == 9:
            print('Faild: predict_race')
            return     
        time.sleep(10) 
    print('Success:predict_race')
    
    # betting = Betting(place_id, race_no)
    # betting.open_betting_page()
    
    while True:
        if (datetime.datetime.now()+datetime.timedelta(minutes=1)).strftime('%H:%M') == vote_deadline:
            res = requests.post(
                                get_expected_value_url,
                                json=body
                                )
            data = json.loads(res.text.replace("'", '"')) 
            print(data) 
            sub_number = 0
            bet_list = []
            for key, value in data.items():
                # data = value.replace('[', '').replace(']', '').replace(' ', '').split(',')
                data = value
                pred = data[0]
                odds = data[1]
                ex_value = data[2]
                if ex_value>ex_th and pred >= 0.02:
                    bet = {
                        'bet_type': 1 ,
                        'bracket1': key,
                        'bracket2': None,
                        'bracket3': None,
                        'amount' : 1 if int(ex_return/(odds*100)) == 0 else int(ex_return/(odds*100))
                        }
                    sub_number += 1
                    print(bet)
                    bet_list.append(bet)
                    
            # betting = Betting(place_id, race_no)
            # betting.do_bet(bet_list)
            tweet_result(race_date, place_id, race_no, bet_list)
            break
        time.sleep(5)
    
    
    print('Done: ', race_date, place_id, race_no)
    
def update_race_data():    
    requests.post(
                update_race_date_url,
                json={}
                )
def main():
    while True:
        try:
            racetime_list = get_racetime_list()
            print(racetime_list)
            racetime_list = list(filter(lambda x: x['vote_deadline']==(datetime.datetime.now()+datetime.timedelta(minutes=5)).strftime('%H:%M'), racetime_list))
            print(racetime_list)
            for race in racetime_list:
                print('predict: ', race)
                thread = threading.Thread(target=thred_tsk, args=([datetime.datetime.now().strftime('%Y%m%d'), race['place_id'], race['race_no'], race['vote_deadline']]))
                thread.start()
                thread = threading.Thread(target=update_race_data)
                thread.start()
            time.sleep(60)
        except KeyboardInterrupt:
            print("Ctrl-c pressed ...")
            sys.exit()
        except:
            traceback.print_exc()
            print('error')
        
if __name__== '__main__':
    main()