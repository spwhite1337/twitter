import re
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
    def remove_emojis(text: str) -> str:
        emoj = re.compile("["
                          u"\U0001F600-\U0001F64F"  # emoticons
                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                          u"\U0001F680-\U0001F6FF"  # transport & map symbols
                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                          u"\U00002500-\U00002BEF"  # chinese char
                          u"\U00002702-\U000027B0"
                          u"\U00002702-\U000027B0"
                          u"\U000024C2-\U0001F251"
                          u"\U0001f926-\U0001f937"
                          u"\U00010000-\U0010ffff"
                          u"\u2640-\u2642"
                          u"\u2600-\u2B55"
                          u"\u200d"
                          u"\u23cf"
                          u"\u23e9"
                          u"\u231a"
                          u"\ufe0f"  # dingbats
                          u"\u3030"
                          "]+", re.UNICODE)
        return re.sub(emoj, '', text).strip()

    @staticmethod
    def _is_airport_format(version: str, tweet_line: str) -> bool:
        if version == 'v1':
            tweet_line_parts = tweet_line.split('-')
            # Should have two parts
            split_by_dash = len(tweet_line_parts) == 2
            if not split_by_dash:
                return split_by_dash

            # First part is airport code
            airport_code = tweet_line_parts[0].strip()
            is_airport_code = bool(re.match(r'[A-Z]{3}', airport_code))
            # Second part is a time
            is_time = re.match(r'^[0-9]{1,2}:[0-9]{2}[a,p]m [A-Z]{2}', tweet_line_parts[1].strip())

            return is_airport_code and is_time
        else:
            return False

    @staticmethod
    def _airport_format(version: str, tweet_line: str) -> Dict[str, str]:
        if version == 'v1':
            tweet_line_parts = tweet_line.split('-')
            return {
                'airport_code': tweet_line_parts[0].strip(),
                'airport_time': tweet_line_parts[1].strip()
            }
        else:
            return {}

    def _parse_tweet(self, version: str, tweet: str) -> Dict[str, str]:
        """
        Parse a tweet based on observed logic
        """
        result = {}
        if version == 'v1':
            # iterate over the lines in the tweet
            for tweet_line in tweet.split('\n'):
                # Strip emojis
                tweet_line = self.remove_emojis(tweet_line)

                # Add airport information
                if self._is_airport_format(version, tweet_line):
                    result.update(self._airport_format(version, tweet_line))

        return result

    @staticmethod
    def _parse_for_tail_number(version: str, tweet: str) -> str:
        if version == 'v1':
            # Read lines
            flight_info = tweet.split('\n')
            if len(flight_info) >= 5:
                # Get 4th line up
                flight_info = flight_info[-4][1:]  # Drop emoji in front
                flight_info = flight_info.split('|')
                if len(flight_info) >= 1:
                    return flight_info[0].strip()[1:].strip()
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
            'is_quote_status': [tweet['is_quote_status'] for tweet in tweets],
        })

        # Define parsing versions
        df['p_version'] = df['created_at'].apply(self._parsing_versions)

        # Parse data from tweet
        df['is_retweet'] = df['tweet'].apply(lambda t: t.startswith('RT @'))
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
        ) & (
            ~df['is_retweet']
        )

        return df.drop_duplicates().reset_index(drop=True)
