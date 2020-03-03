import requests
import datetime
import sys
import traceback
import logging
import pprint

logger = logging.getLogger(__name__)
fh = logging.FileHandler('./log/request_crawling.log')
logger.addHandler(fh)
formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
fh.setFormatter(formatter)
logger.setLevel(logging.INFO)

clawling_api_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/boat_clawling'
get_course_api_url = 'https://asia-northeast1-maximal-boulder-268803.cloudfunctions.net/get_course_list'

def main():
    logger.info('Process start!!')

    dt = datetime.datetime(2020,2,28)
    while dt.strftime('%Y%m%d') != '20190101':
        race_date = dt.strftime('%Y%m%d')
        print("race_date: ", race_date)
        try:
            res = requests.post(
                get_course_api_url,
                json={"race_date":race_date}
            )
            if res.status_code == 200:
                place_id_list = res.text.replace("'","").replace(' ','').split(',')
            
            else:
                continue

            for place_id in place_id_list:
                for race_no in [str(i+1) for i in range(12)]:
                    res = requests.post(
                        clawling_api_url,
                        json={"race_date":race_date,
                              "place_id": place_id,
                              "race_no": race_no
                            }
                    )
                    print(res.text)
                    print('place_id: ', place_id, 'race_no: ', race_no, res.status_code)



        except KeyboardInterrupt:
            print("Ctrl-c pressed ...")
            logger.info("Ctrl-c pressed ...")
            logger.info('Process end!!')
            sys.exit()
        except:
            print(race_date)
            e = traceback.format_exc()
            print(e)
            logger.error(e)
            logger.info('Process end!!')

            
        dt -= datetime.timedelta(days=1)
        
if __name__ == '__main__':

    main()
    