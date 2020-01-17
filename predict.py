import pandas as pd
import numpy as np
import copy
import os
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from IPython.display import clear_output
import crawling_boat as cb
import math
import gc

import config.config as cfg
import util.modules as mdl

LOAD_DATA_COLUMNS = ['race_date', 'place_id', 'race_no', 'race_date_no', 'bracket_no', 'is_miss', 'player_id', 'player_grade', 'branch', 'born_area', 'age', 'weight', 'f_count', 'l_count', 'start_time_avg', 'first_rate_all', 'second_rate_all', 'third_rate_all', 'first_rate_area', 'second_rate_area', 'third_rate_area', 'motor_no', 'motor_within_second_rate', 'motor_within_third_rate', 'boat_no', 'boat_within_second_rate', 'boat_within_third_rate', 'pre_time', 'tilt_angle', 'propeller', 'parts', 'adjust_weight', 'pre_start_timing', 'finish_order', 'player_race_time', 'start_timing', 'win_pattern', 'race_grade', 'distance', 'course_direction', 'weather', 'temperature', 'wind', 'wind_direction', 'water_temperature', 'wave_height', 'race_date_old1', 'place_id_old1', 'race_no_old1', 'bracket_no_old1', 'is_miss_old1', 'player_id_old1', 'player_grade_old1', 'branch_old1', 'born_area_old1', 'age_old1', 'weight_old1', 'f_count_old1', 'l_count_old1', 'start_time_avg_old1', 'first_rate_all_old1', 'second_rate_all_old1', 'third_rate_all_old1', 'first_rate_area_old1', 'second_rate_area_old1', 'third_rate_area_old1', 'motor_no_old1', 'motor_within_second_rate_old1', 'motor_within_third_rate_old1', 'boat_no_old1', 'boat_within_second_rate_old1', 'boat_within_third_rate_old1', 'pre_time_old1', 'tilt_angle_old1', 'propeller_old1', 'parts_old1', 'adjust_weight_old1', 'pre_start_timing_old1', 'finish_order_old1', 'player_race_time_old1', 'start_timing_old1', 'win_pattern_old1', 'race_date_old2', 'place_id_old2', 'race_no_old2', 'bracket_no_old2', 'is_miss_old2', 'player_id_old2', 'player_grade_old2', 'branch_old2', 'born_area_old2', 'age_old2', 'weight_old2', 'f_count_old2', 'l_count_old2', 'start_time_avg_old2', 'first_rate_all_old2', 'second_rate_all_old2', 'third_rate_all_old2', 'first_rate_area_old2', 'second_rate_area_old2', 'third_rate_area_old2', 'motor_no_old2', 'motor_within_second_rate_old2', 'motor_within_third_rate_old2', 'boat_no_old2', 'boat_within_second_rate_old2', 'boat_within_third_rate_old2', 'pre_time_old2', 'tilt_angle_old2', 'propeller_old2', 'parts_old2', 'adjust_weight_old2', 'pre_start_timing_old2', 'finish_order_old2', 'player_race_time_old2', 'start_timing_old2', 'win_pattern_old2', 'race_date_old3', 'place_id_old3', 'race_no_old3', 'bracket_no_old3', 'is_miss_old3', 'player_id_old3', 'player_grade_old3', 'branch_old3', 'born_area_old3', 'age_old3', 'weight_old3', 'f_count_old3', 'l_count_old3', 'start_time_avg_old3', 'first_rate_all_old3', 'second_rate_all_old3', 'third_rate_all_old3', 'first_rate_area_old3', 'second_rate_area_old3', 'third_rate_area_old3', 'motor_no_old3', 'motor_within_second_rate_old3', 'motor_within_third_rate_old3', 'boat_no_old3', 'boat_within_second_rate_old3', 'boat_within_third_rate_old3', 'pre_time_old3', 'tilt_angle_old3', 'propeller_old3', 'parts_old3', 'adjust_weight_old3', 'pre_start_timing_old3', 'finish_order_old3', 'player_race_time_old3', 'start_timing_old3', 'win_pattern_old3', 'race_grade_old1',
                     'distance_old1', 'course_direction_old1', 'weather_old1', 'temperature_old1', 'wind_old1', 'wind_direction_old1', 'water_temperature_old1', 'wave_height_old1', 
                     'race_grade_old2', 'distance_old2', 'course_direction_old2', 'weather_old2', 'temperature_old2', 'wind_old2', 'wind_direction_old2', 'water_temperature_old2', 'wave_height_old2', 
                     'race_grade_old3', 'distance_old3', 'course_direction_old3', 'weather_old3', 'temperature_old3', 'wind_old3', 'wind_direction_old3', 'water_temperature_old3', 'wave_height_old3']

PARTS = {'キャリボ': 4,
                 'ピストン': 4,
                 'リング': 3,
                 '電気': 2, 
                 'キャブ': 3,
                 'ギヤ': 2,
                 'シリンダ': 4,
                 'シャフト': 4}

class Predict:
    
    def __init__(self, race_date, place_id, race_no):
        self.race_date = race_date
        self.place_id = place_id
        self.race_no = race_no
        
        self.dtypes = pd.read_csv('csv/dtypes.csv')
        
    def insert_racedate_to_df(self):
        # TODO
        print('test')
        
    def get_predict_data(self):
        # データ取得クエリ作成
        with open(os.path.join('sql/', 'select_predict_data.sql')) as f:
            query = f.read()

        race_inf = {
                    'race_date': self.race_date,
                    'place_id': self.place_id,
                    'race_no': self.race_no
                }
        
        query = query.format(**race_inf).replace('\n', ' ')
        
        #クエリ実行
        engine = mdl.load_engine()
        res = engine.execute(query)
        
        return [r for r in res]
    
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
            self.df[key+'_count'] = self.df['parts'].apply(mdl.parts_count, key=key, num=PARTS[key])
            self.df[key+'_flg_old1'] = self.df['parts_old1'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count_old1'] = self.df['parts_old1'].apply(mdl.parts_count, key=key, num=PARTS[key])
            self.df[key+'_flg_old2'] = self.df['parts_old2'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count_old2'] = self.df['parts_old2'].apply(mdl.parts_count, key=key, num=PARTS[key])
            self.df[key+'_flg_old3'] = self.df['parts_old3'].apply(lambda x: True if str(x).find(key)!=-1 else False)
            self.df[key+'_count_old3'] = self.df['parts_old3'].apply(mdl.parts_count, key=key, num=PARTS[key])
            
        # 枠番
        self.df['bracket_num'] = self.df['bracket_no']
        self.df['bracket_num_old1'] = self.df['bracket_no_old1']
        self.df['bracket_num_old2'] = self.df['bracket_no_old2']
        self.df['bracket_num_old3'] = self.df['bracket_no_old3']
        
        # 過去レースタイム変換
        self.df['player_race_time_old1'] = self.df['player_race_time_old1'].apply(mdl.conv_time)
        self.df['player_race_time_old2'] = self.df['player_race_time_old2'].apply(mdl.conv_time)
        self.df['player_race_time_old3'] = self.df['player_race_time_old3'].apply(mdl.conv_time)
        
        # 特選フラグ
        # TODO
        
        # 前走からの日付
        #コンドツクル
        
        # ターゲットエンコーディング
        group = self.df.groupby(['bracket_no'])
        # 枠順ごとの着順平均
        bracket_order_mean = pd.read_csv('csv/bracket_order_mean.csv')
        bracket_order_mean['bracket_no'] = bracket_order_mean.index+1
        self.df = pd.merge(self.df, bracket_order_mean, on='bracket_no')
        # 枠順ごとの着順中央値
        bracket_order_median = pd.read_csv('csv/bracket_order_median.csv')
        bracket_order_median['bracket_no'] = bracket_order_median.index+1
        self.df = pd.merge(self.df, bracket_order_median, on='bracket_no')
        
        self.df = self.df.sort_values(by=['race_date', 'place_id', 'race_no', 'bracket_no'])
        
        # n走前nullフラグ作成
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
        
        # レース情報
        race_list = self.df[['race_date', 'place_id', 'race_no', 'bracket_no']]
        
        #One-Hot Encoding
        dummies_list = ['place_id','bracket_no','player_grade','branch','born_area','propeller','weather']
        past_dummies_list = ['place_id','bracket_no','player_grade','branch','born_area','propeller','weather','win_pattern']
        
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
                            'motor_no','boat_no','finish_order','player_race_time','race_grade',
                            'start_timing','win_pattern','race_date_old1',
                            'is_miss_old1','player_id_old1','motor_no_old1',
                            'boat_no_old1','race_date_old2','is_miss_old2',
                            'player_id_old2','motor_no_old2','boat_no_old2',
                            'race_date_old3','is_miss_old3','player_id_old3',
                            'motor_no_old3','boat_no_old3',
                            'race_grade_old1', 'race_grade_old2', 'race_grade_old3']
        self.df.drop(drop_list, axis = 1, inplace=True)
        
        input_shape = pd.read_csv('csv/input_shape.csv')
        add_columns = list(filter(lambda x: x not in self.df.columns, input_shape.columns))
        
        self.df = pd.concat([self.df,input_shape])[input_shape.columns]
        
        for c in add_columns:
            self.df[c] = self.df[c].fillna(0)
        
    
    def weight_to_int(self):
        self.df['weight'] = self.df['weight'].apply(lambda x: math.floor(float(x)))
        self.df['weight_old1'] = self.df['weight_old1'].apply(lambda x: math.floor(float(x)))
        self.df['weight_old2'] = self.df['weight_old2'].apply(lambda x: math.floor(float(x)))
        self.df['weight_old3'] = self.df['weight_old3'].apply(lambda x: math.floor(float(x)))
        
    def align_dtypes(self):
        for i, c in enumerate(LOAD_DATA_COLUMNS):
            self.df[c] = self.df[c].astype(self.dtypes[i])
            
    def load_model(self, path):
        self.model = lgb.Booster(model_file=path)
        
    def get_pred(self):
        output = self.model.predict(self.df.values)
        return output[:,1]/sum(output[:,1])

    def main_proess(self):
        # DB更新
        #  self.insert_racedate_to_df()
        
        # 予測用データ取得
        self.df = pd.DataFrame(self.get_predict_data(), columns=LOAD_DATA_COLUMNS)
        
        # 学習データで選手の体重がintになってたから揃える
        # 次直す
        self.weight_to_int()
        
        # dtypeを学習用データに揃える
        self.align_dtypes()
        
        # データ前処理
        self.preprocessing()
        
        # モデル読み込み
        self.load_model('model/first_model_target.txt')
        
        print(self.get_pred())

if __name__ == '__main__':
    predict = Predict('20200113', '16', '1')
    
    predict.main_proess()