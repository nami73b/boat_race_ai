from sqlalchemy import create_engine
import numpy as np
import config.config as cfg
from bs4 import BeautifulSoup
from urllib.request import urlopen


def load_engine():
    db_settings = cfg.db_settings
    return create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={encoding}'.format(**db_settings))

def get_page_source(url):
    html = urlopen(url)
    soup = BeautifulSoup(html,"html.parser")
    return soup

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

def parts_count(x, key, num):
    x = str(x)
    if x.find(key) != -1:
        if x[x.find(key):].find('×') != -1:
            return x[x.find(key)+num+1]
        else:
            return 1
    else:
        return 0
    

def drop_miss(df):
    return df[df['is_miss'] == False]

def conv_time(x):
    if str(x).find('nan') != -1:
        return x
    elif str(x).find(':') == -1:
        return np.nan
    else:
        time = x.split(':')
        return float(time[0])*60 + float(time[1]) + float(time[2])/10