import os
import json
import tweepy
import datetime
import pandas as pd
from typing import List, Dict, Optional


from twitter.config import Config, logger


class ETL(object):
    """
    Pull data from Twitter
    """
    version = 'v1'
    twitter_dev_env = 'marcus'
    min_date = pd.Timestamp('2017-01-01')
    max_tweets = 100000
    figsize = (12, 12)

    def __init__(self, screen_name: str):
        self.screen_name = screen_name
        auth = tweepy.OAuthHandler(os.environ['consumer_key'], os.environ['consumer_secret'])
        auth.set_access_token(os.environ['access_token_key'], os.environ['access_token_secret'])
        self.api = tweepy.API(auth)
        if not self.api.verify_credentials():
            logger.info('Could not Validate Credentials')
        self.save_dir = os.path.join(Config.DATA_DIR, self.screen_name)
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def etl(self, to_date: Optional[str] = None, overwrite: bool = False) -> List[Dict]:
        """
        Pull tweets, save raw, and pull json
        """
        if os.path.exists(os.path.join(self.save_dir, 'raw_tweets_{}.json'.format(self.version))) and not overwrite:
            logger.info('Loading Raw Tweets from Cache')
            with open(os.path.join(self.save_dir, 'raw_tweets_{}.json'.format(self.version)), 'r') as jp:
                tweets = json.load(jp)
            return tweets

        logger.info('Pulling Tweets for {}'.format(self.screen_name))
        tweets = []
        to_date = pd.Timestamp(datetime.datetime.now()) if to_date is None else pd.Timestamp(to_date)
        while to_date > self.min_date and len(tweets) < self.max_tweets:
            # Iterate over 500 tweet batches
            tweet_times = []
            for status in tweepy.Cursor(
                    self.api.search_full_archive,
                    label=self.twitter_dev_env,
                    query='from:{}'.format(self.screen_name),
                    toDate=to_date.strftime('%Y%m%d%H%M'),
                    # fromDate=self.min_date.strftime('%Y%m%d%H%M'),
                    maxResults=500
            ).items():
                tweets.append(status._json)
                tweet_times.append(pd.Timestamp(status._json['created_at']).tz_localize(None))
            if len(tweet_times) == 0:
                to_date = to_date - pd.Timedelta(days=30)
                logger.info('No tweets from {} to {}'.format(to_date + pd.Timedelta(days=30), to_date))
            else:
                to_date = min(tweet_times)
            logger.info('{} Total Tweets back to {}'.format(len(tweets), to_date))

        # If you set a limit and the old data exists, then append them together
        if to_date is not None and \
                os.path.exists(os.path.join(self.save_dir, 'raw_tweets_{}.json'.format(self.version))):
            logger.info('Appending Old Tweets to New')
            with open(os.path.join(self.save_dir, 'raw_tweets_{}.json'.format(self.version)), 'r') as jp:
                old_tweets = json.load(jp)
            tweets.extend(old_tweets)
        logger.info('Saving Raw Tweets')
        with open(os.path.join(self.save_dir, 'raw_tweets_{}.json'.format(self.version)), 'w') as jp:
            json.dump(tweets, jp)
        return tweets

    def save_parsed(self, df: pd.DataFrame):
        """
        Save parsed tweets
        """
        logger.info('Saving Parsed Tweets')
        df_nones = df[
            (df['flightware_links'] == 'None') | (df['team_names'] != 'None')
        ]
        df = df[
            (df['flightware_links'] != 'None') & (df['team_names'] != 'None')
        ]
        df_unparsed = df[~df['parsed']].drop('parsed', axis=1)
        df = df[df['parsed']].drop('parsed', axis=1)
        df = df.sort_values('created_at')
        df_nones = df_nones.sort_values('created_at')
        df_nones.to_csv(os.path.join(self.save_dir, 'nones_{}.csv'.format(self.version)), index=False)
        df_unparsed.to_csv(os.path.join(self.save_dir, 'unparsed_tweets_{}.csv'.format(self.version)), index=False)
        df.to_csv(os.path.join(self.save_dir, 'parsed_tweets_{}.csv'.format(self.version)), index=False)
        df.to_excel(os.path.join(self.save_dir, 'parsed_tweets_{}.xlsx'.format(self.version)), index=False)

