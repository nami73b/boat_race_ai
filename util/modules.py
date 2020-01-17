from sqlalchemy import create_engine
import config.config as cfg

def load_engine():
    db_settings = cfg.db_settings
    return create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={encoding}'.format(**db_settings))

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