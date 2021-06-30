import os
import requests
import pandas as pd
import psutil
import json

# constants
from util.BucketUpload import FileUtil

FILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'files')
BASE_URL = 'https://fantasydata.com/NFL_Adp/ADP_Read'
FILE_NAME = 'adp_year.csv'


class PlayerADP:
    @staticmethod
    def get_player_adp():
        data_frames = []

        # Iterate each year and call ADP API
        for year in range(int(os.getenv('START_YEAR', 2014)), int(os.getenv('END_YEAR', 2022))):
            r = requests.post(BASE_URL, data={'filters.season': year})

            # build dataframe and append year
            data_json = json.loads(r.text)

            if data_json['Total'] != 0:
                df = pd.json_normalize(data_json['Data'])
                df['year'] = year

                data_frames.append(df)

            print('ADP stats processed for: {}. CPU%: {}. Memory: {}'.format(year, psutil.cpu_percent(),
                                                                             dict(psutil.virtual_memory()._asdict())))

        # concat dataframes and write to file
        df = pd.concat(data_frames, axis=0)
        df.to_csv(os.path.join(FILES_DIR, FILE_NAME), index=False, encoding='utf-8')

        print('ADP year written')

        # upload to bucket here
        FileUtil().upload_to_bucket(FILE_NAME, os.path.join(FILES_DIR, FILE_NAME), os.getenv('FANTASY_DATA_BUCKET', 'fantasy-year-data'))
        print('ADP File uploaded to bucket')

        print('ADP processing complete')
