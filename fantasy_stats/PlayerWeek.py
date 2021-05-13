import os
import requests
import pandas as pd
import psutil
import json


# constants
FILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'files')
BASE_URL = 'https://fantasydata.com/NFL_FantasyStats/FantasyStats_Read'
FILE_NAME = 'player_fantasy_week.csv'


class PlayerWeek:
    @staticmethod
    def get_week_stats():
        data_frames = []

        # Iterate each year and call ADP API
        for year in range(int(os.getenv('START_YEAR', 1992)), int(os.getenv('END_YEAR', 2021))):
            params = {"filters.seasontype": "1", "filters.scope": "2", "filters.subscope": "1",
                      "filters.startweek": "1",
                      "filters.endweek": "17", "filters.aggregatescope": "1", "filters.range": "1",
                      'filters.season': year}

            r = requests.post(BASE_URL, data=params)

            # build dataframe and append year
            data_json = json.loads(r.text)

            if data_json['Total'] != 0:
                df = pd.json_normalize(data_json['Data'])
                df['year'] = year

                data_frames.append(df)

            print('Player Fantasy Week stats processed ({}) for: {}. CPU%: {}. Memory: {}'.format(data_json['Total'],
                                                                                                  year,
                                                                                                  psutil.cpu_percent(),
                                                                                                  dict(
                                                                                                      psutil.virtual_memory()._asdict())))

        # concat dataframes and write to file
        df = pd.concat(data_frames, axis=0)
        df.to_csv(os.path.join(FILES_DIR, FILE_NAME), index=False, encoding='utf-8')

        print('Player Fantasy Week file written')

        # todo upload to bucket here
        # print('Player Fantasy Week file uploaded to bucket')

        print('Player Fantasy Week processing complete')



