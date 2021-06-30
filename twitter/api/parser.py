from typing import List, Dict, Any, Tuple, Optional
import pandas as pd

from twitter.config import logger
from twitter.api.etl import ETL


class Parser(ETL):
    """
    Parse the data from twitter
    """
    @staticmethod
    def _get_user_mentions(tweet: Dict[str, Any]) -> str:
        mentions = tweet.get('entities', {}).get('user_mentions', [])
        if len(mentions) > 0:
            return mentions[0].get('screen_name', 'None')
        else:
            return 'None'

    @staticmethod
    def _get_team_names(tweet: Dict[str, Any]) -> str:
        mentions = tweet.get('entities', {}).get('user_mentions', [])
        if len(mentions) > 0:
            return mentions[0].get('name', 'None')
        else:
            return 'None'

    @staticmethod
    def _get_flightware_links(tweet: Dict[str, Any]) -> str:
        # Get first link for flightware
        urls = tweet.get('entities', []).get('urls', [])
        if len(urls) > 0:
            return urls[0].get('url', 'None')
        else:
            return 'None'

    @staticmethod
    def _parsing_versions(created_at: pd.Timestamp) -> str:
        if created_at > pd.Timestamp('2021-01-03 00:00:00'):
            return 'v1'
        else:
            return 'v2'

    @staticmethod
    def _parse_for_tail_number(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 5:
                flight_info = flight_info[-4][1:]  # Drop emoji in front
                flight_info = flight_info.split('|')
                if len(flight_info) >= 3:
                    return flight_info[0].strip()[1:]
            return 'None'
        else:
            return 'None'

    @staticmethod
    def _parse_for_flight_no(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 5:
                flight_info = flight_info[-4][1:]  # Drop emoji in front
                flight_info = flight_info.split('|')
                if len(flight_info) >= 3:
                    return flight_info[1].strip()
            return 'None'
        else:
            return 'None'

    @staticmethod
    def _parse_for_aircraft_type(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 5:
                flight_info = flight_info[-4][1:]  # Drop emoji in front
                flight_info = flight_info.split('|')
                if len(flight_info) >= 3:
                    return flight_info[2].strip()
            return 'None'
        else:
            return 'None'

    @staticmethod
    def _parse_for_departure(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 3:
                flight_info = flight_info[-3][1:]  # Drop emoji in front
                flight_info = flight_info.split(' - ')
                if len(flight_info) >= 1:
                    return flight_info[0].strip()
            return 'None'
        else:
            return 'None'

    @staticmethod
    def _parse_for_departure_time(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 3:
                flight_info = flight_info[-3][1:]  # Drop emoji in front
                flight_info = flight_info.split(' - ')
                if len(flight_info) >= 2:
                    return flight_info[1].strip()
            return 'None'
        else:
            return 'None'

    @staticmethod
    def _parse_for_arrival(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 3:
                flight_info = flight_info[-2][1:]  # Drop emoji in front
                flight_info = flight_info.split(' - ')
                if len(flight_info) >= 1:
                    return flight_info[0].strip()
            return 'None'
        else:
            return 'None'

    @staticmethod
    def _parse_for_arrival_time(version: str, tweet: str) -> str:
        if version == 'v1':
            flight_info = tweet.split('\n')
            if len(flight_info) >= 3:
                flight_info = flight_info[-2][1:]  # Drop emoji in front
                flight_info = flight_info.split(' - ')
                if len(flight_info) >= 2:
                    return flight_info[1].strip()
            return 'None'
        else:
            return 'None'

    def parse_raw_tweets(self, tweets: Optional[List[Dict[str, Any]]] = None) -> pd.DataFrame:
        """
        Parse raw tweets from jsons
        """
        if tweets is None:
            tweets = self.etl()

        # Extract data from response object
        logger.info('Parsing {} Tweets'.format(len(tweets)))
        df = pd.DataFrame({
            'created_at': [pd.Timestamp(tweet['created_at']).tz_localize(None) for tweet in tweets],
            'tweet': [tweet.get('extended_tweet', {'full_text': tweet['text']})['full_text'] for tweet in tweets],
            'user_mentions': [self._get_user_mentions(tweet) for tweet in tweets],
            'team_names': [self._get_team_names(tweet) for tweet in tweets],
            'flightware_links': [self._get_flightware_links(tweet) for tweet in tweets],
            'retweets': [tweet['retweet_count'] for tweet in tweets],
            'favorite_count': [tweet['favorite_count'] for tweet in tweets],
            'is_reply': [tweet['in_reply_to_user_id'] is not None for tweet in tweets],
            'is_quote_status': [tweet['is_quote_status'] for tweet in tweets]
        })

        # Define parsing versions
        df['p_version'] = df['created_at'].apply(self._parsing_versions)

        # Parse data from tweet
        df['tail_number'] = df.apply(lambda r: self._parse_for_tail_number(r['p_version'], r['tweet']), axis=1)
        df['flight_no'] = df.apply(lambda r: self._parse_for_flight_no(r['p_version'], r['tweet']), axis=1)
        df['aircraft_type'] = df.apply(lambda r: self._parse_for_aircraft_type(r['p_version'], r['tweet']), axis=1)
        df['departure'] = df.apply(lambda r: self._parse_for_departure(r['p_version'], r['tweet']), axis=1)
        df['departure_time'] = df.apply(lambda r: self._parse_for_departure_time(r['p_version'], r['tweet']), axis=1)
        df['arrival'] = df.apply(lambda r: self._parse_for_arrival(r['p_version'], r['tweet']), axis=1)
        df['arrival_time'] = df.apply(lambda r: self._parse_for_arrival_time(r['p_version'], r['tweet']), axis=1)

        df['parsed'] = (
            df['tail_number'] != 'None'
        ) & (
            df['flight_no'] != 'None'
        ) & (
            df['aircraft_type'] != 'None'
        ) & (
            df['departure'] != 'None'
        ) & (
            df['departure_time'] != 'None'
        ) & (
            df['arrival'] != 'None'
        ) & (
            df['arrival_time'] != 'None'
        ) & (
            ~df['is_reply']
        ) & (
            ~df['is_quote_status']
        )

        return df.drop_duplicates().reset_index(drop=True)
