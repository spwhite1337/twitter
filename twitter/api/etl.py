import os
import json
import tweepy
import datetime
import pandas as pd
from typing import List, Dict, Tuple, Any
from tqdm import tqdm
from twitter.config import Config, logger


class ETL(object):
    """
    Pull data from Twitter
    """
    twitter_dev_env = 'marcus'
    min_date = pd.Timestamp('2019-01-01')
    max_tweets = 100000

    def __init__(self, screen_name: str, overwrite: bool = False):
        self.screen_name = screen_name
        auth = tweepy.OAuthHandler(os.environ['consumer_key'], os.environ['consumer_secret'])
        auth.set_access_token(os.environ['access_token_key'], os.environ['access_token_secret'])
        self.api = tweepy.API(auth)
        if not self.api.verify_credentials():
            logger.info('Could not Validate Credentials')
        self.overwrite = overwrite
        self.save_dir = os.path.join(Config.DATA_DIR, self.screen_name)
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def etl(self) -> List[Dict]:
        """
        Pull tweets, save raw, and pull json
        """
        logger.info('Pulling Tweets for {}'.format(self.screen_name))
        tweets = []
        to_date = pd.Timestamp(datetime.datetime.now())
        while to_date > self.min_date and len(tweets) < self.max_tweets:
            # Iterate over 500 tweet batches
            tweet_times = []
            for status in tqdm(tweepy.Cursor(
                    self.api.search_full_archive,
                    label=self.twitter_dev_env,
                    query='from:{}'.format(self.screen_name),
                    toDate=to_date.strftime('%Y%m%d%H%M'),
                    maxResults=500
            ).items()):
                tweets.append(status._json)
                tweet_times.append(pd.Timestamp(status._json['created_at']).tz_localize(None))
            if len(tweet_times) == 0:
                to_date = self.min_date
            else:
                to_date = min(tweet_times)

        logger.info('Saving Raw Tweets')
        with open(os.path.join(self.save_dir, 'raw_tweets.json'), 'w') as jp:
            json.dump(tweets, jp)
        return tweets
