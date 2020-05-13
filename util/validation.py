import pandas as pd

class validation:
    def __init__(self):
        self.df = pd.read_csv('csv/race_payoff.csv')

    def betting(self, bet_dct: dict) -> list:
        #### input ####
        #     bet_dct = {'race_date': YYYYMMDD,
        #                        'place_id': ,
        #                        'race_no': ,
        #                        'bet_type': ,
        #                        'bracket1': ,
        #                        'bracket2': ,
        #                        'bracket3': ,
        #                        'amount': 
        #                        }
        
        #### output ####
        # [buyying, dividend]
        # buyying -> 購入額
        # dividend ->　配当額

        # 購入額
        buyying = 0
        # 配当額
        dividend = 0
        
        payoff = self.df[(self.df['race_date'] == bet_dct['race_date'])
                    & (self.df['place_id'] == bet_dct['place_id'])
                    & (self.df['race_no'] == bet_dct['race_no'])
                    & (self.df['bet_type'] == bet_dct['bet_type'])]
        
        buyying += bet_dct['amount']*100
        
        if bet_dct['bet_type'] in [1, 2]:
            # 単勝, 複勝
            for i in payoff.index:
                if payoff.ix[i]['bracket1'] == bet_dct['bracket1']:
                    dividend += payoff.ix[i]['payoff']*bet_dct['amount']
                    
        elif bet_dct['bet_type'] in [3, 4]:
            # 二連複, ワイド
            for i in payoff.index:
                hit_list = [payoff.ix[i]['bracket1'], payoff.ix[i]['bracket2']]
                if (bet_dct['bracket1'] in hit_list) and (bet_dct['bracket2'] in hit_list):
                    dividend += payoff.ix[i]['payoff']*bet_dct['amount']
                
        elif bet_dct['bet_type'] in [5]:
            # 二連単
            for i in payoff.index:
                if bet_dct['bracket1'] == payoff.ix[i]['bracket1'] \
                  and bet_dct['bracket2'] == payoff.ix[i]['bracket2']:
                    dividend += payoff.ix[i]['payoff']*bet_dct['amount']
        
        elif bet_dct['bet_type'] in [6]:
            # 三連複
            for i in payoff.index:
                hit_list = [payoff.ix[i]['bracket1'], payoff.ix[i]['bracket2'], payoff.ix[i]['bracket3']]
                if (bet_dct['bracket1'] in hit_list) \
                   and (bet_dct['bracket2'] in hit_list) \
                   and (bet_dct['bracket3'] in hit_list):
                    dividend += payoff.ix[i]['payoff']*bet_dct['amount']
                
        elif bet_dct['bet_type'] in [7]:
            # 三連単
            for i in payoff.index:
                if bet_dct['bracket1'] == payoff.ix[i]['bracket1'] \
                  and bet_dct['bracket2'] == payoff.ix[i]['bracket2'] \
                  and bet_dct['bracket3'] == payoff.ix[i]['bracket3']:
                    dividend += payoff.ix[i]['payoff']*bet_dct['amount']
            
        else:
            return [0, 0]
        
        return [buyying, dividend]