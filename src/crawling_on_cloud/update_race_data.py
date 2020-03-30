from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import requests
import time
from sqlalchemy import create_engine


url = 'https://www.boatrace.jp/owpc/pc/race/pay?hd={}'
clawling_api_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/boat_clawling'

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

def get_end_race_list():
    html = urlopen(url.format(datetime.datetime.now().strftime('%Y%m%d')))
    soup = BeautifulSoup(html,"html.parser")
    
    end_race_list = list(filter(lambda x: x.find('raceresult')!=-1, set([td.get('data-href') for td in soup.find_all('td', {'class': 'cellbg'})])))
    end_race_list = [list(reversed([r.split('=')[-1] for r in race.split('?')[-1].split('&')])) for race in end_race_list]
    return end_race_list

def get_exist_data_race_list(engine):
    query = """
                select race_date, place_id, race_no from race_payoff
                where race_date = '{}'
            """
    print(query.format(datetime.datetime.now().strftime('%Y%m%d')))
    res = engine.execute(query.format(datetime.datetime.now().strftime('%Y%m%d')))
    return [r for r in res]

def main(request):
    engine = load_engine()
    end_race_list = get_end_race_list()
    exist_data_race_list = get_exist_data_race_list(engine)
    
    race_list = [race for race in end_race_list if race not in exist_data_race_list]
    print(race_list)
    for race in race_list:
        for i in range(10):
            body = {"race_date":race[0],
                    "place_id":race[1],
                    "race_no":race[2]}
            res = requests.post(
                                clawling_api_url,
                                json=body
                                )
            if res.status_code == 200:
                print('success: ', body)
                break
            if i == 9:
                print('Faild: update')
            time.sleep(10)
        