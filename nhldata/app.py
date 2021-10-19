'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

import json
from pprint import pprint

import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        print(url)
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    # TODO what propertie are needed to partition?
    gameid: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        # TODO use the properties to return the s3 key
        return f'{self.gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def parse_player_details(self, side, box_team_players, player_id_str):
        team_players = box_team_players[player_id_str]
        player_person = team_players['person']
        player_person_id = player_person['id']
        player_person_currentTeam_name = player_person['currentTeam']['name']
        player_person_fullName = player_person['fullName']

        player_skaterStats = team_players['stats'].get('skaterStats', None)
        if player_skaterStats:
            player_stats_skaterStats_assists = player_skaterStats['assists']
            player_stats_skaterStats_goals = player_skaterStats['goals']
        else:
            return []
        # print(player_person_id, player_person_currentTeam_name, player_person_fullName, player_stats_skaterStats_assists, player_stats_skaterStats_goals, side)
        return [player_person_id, player_person_currentTeam_name, player_person_fullName, player_stats_skaterStats_assists, player_stats_skaterStats_goals, side]

    def crawl(self, startDate: datetime, endDate: datetime) -> None:
        nhlapi = NHLApi()
        players_data = nhlapi.schedule(startDate, endDate)
        # pprint(players_data)
        dates = players_data['dates']
        records = []
        for date in dates:
            # pprint(date)
            games = date['games']
            event_date = date['date']
            print(f'event_date: {event_date}')
            for game in games:
                games_pk = game['gamePk']
                # teams = game['teams']
                # away_team = teams['away']['team']
                # away_team_id = away_team['id']
                # away_team_name = away_team['name']
                # home_team = teams['home']['team']
                # home_team_id = home_team['id']
                # home_team_name = home_team['name']
                # print(f'\taway_team_id: {away_team_id}, away_team_name: {away_team_name},\n\thome_team_id: {home_team_id},home_team_id: {home_team_name}')

                nhlapi = NHLApi()
                box_score_data = nhlapi.boxscore(games_pk)

                # pprint(box_score_data)

                box_teams = box_score_data['teams']
                box_away_team = box_teams['away']
                box_away_team_players = box_away_team['players']
                box_home_team = box_teams['home']
                box_home_team_players = box_home_team['players']
                for player_id_str in box_away_team_players:
                    away_result = self.parse_player_details("away", box_away_team_players, player_id_str)
                    records.append(away_result)

                for player_id_str in box_home_team_players:
                    home_result = self.parse_player_details("home", box_home_team_players, player_id_str)
                    records.append(home_result)

        # print(records)
        col_names = ["player_person_id", "player_person_currentTeam_name" , "player_person_fullName" , "player_stats_skaterStats_assists" , "player_stats_skaterStats_goals" ,"side"]
        df = pd.DataFrame(records, columns=col_names)
        df = df[df['player_person_id'].notna()]
        df.to_csv('C:\Kalai\Learning\Testing\data-eng-challenge\s3_data\data-bucket\game-stats.csv', index=False, header=False)
        print(df)
        # NOTE the data direct from the API is not quite what we want. Its nested in a way we don't want
        #      so here we are looking for your ability to gently massage a data set.
        #TODO error handling
        #TODO get games for dates
        #TODO for each game get all player stats: schedule -> date -> teams.[home|away] -> $playerId: player_object (see boxscore above)
        #TODO ignore goalies (players with "goalieStats")
        #TODO output to S3 should be a csv that matches the schema of utils/create_games_stats
                 
def main():
    import os
    import argparse
    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    # TODO what arguments are needed to make this thing run,  if any?
    args = parser.parse_args()

    dest_bucket = os.environ.get('DEST_BUCKET', 'output')
    startDate = datetime(2020,8,4)
    endDate = datetime(2020,8,5)
    api = NHLApi()
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)
    crawler.crawl(startDate, endDate)

if __name__ == '__main__':
    main()
