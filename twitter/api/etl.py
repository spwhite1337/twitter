import os
import tweepy
import json
import pickle
import argparse
import pandas as pd
from typing import List, Dict, Tuple, Any
from tqdm import tqdm
from twitter.config import Config, logger


class ETL(object):
    """
    Pull data from Twitter
    """
    def __init__(self, screen_name: str, overwrite: bool = False):
        self.screen_name = screen_name
        auth = tweepy.OAuthHandler(os.environ['consumer_key'], os.environ['consumer_secret'])
        auth.set_access_token(os.environ['access_token_key'], os.environ['access_token_secret'])
        self.api = tweepy.API(auth)
        self.overwrite = overwrite
        self.save_dir = os.path.join(Config.DATA_DIR, self.screen_name)
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def etl(self) -> List[Dict]:
        """
        Pull tweets, save raw, and pull json
        """
        logger.info('Pulling Tweets for {}'.format(self.screen_name))
        statuses = []
        for status in tqdm(tweepy.Cursor(
                self.api.user_timeline,
                screen_name=self.screen_name,
                tweet_mode='extended'
        ).items()):
            statuses.append(status._json)
        logger.info('Saving Raw Tweets')
        with open(os.path.join(self.save_dir, 'raw_tweets.json'), 'w') as jp:
            json.dump(statuses, jp)
        return statuses
