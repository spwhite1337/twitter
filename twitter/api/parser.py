from typing import List, Dict, Any, Tuple, Optional
import pandas as pd

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
    def _parse_for_tail_number(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 5:
            flight_info = flight_info[-4][1:]  # Drop emoji in front
            flight_info = flight_info.split('|')
            if len(flight_info) >= 3:
                return flight_info[0].strip()[1:]
        return 'None'

    @staticmethod
    def _parse_for_flight_no(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 5:
            flight_info = flight_info[-4][1:]  # Drop emoji in front
            flight_info = flight_info.split('|')
            if len(flight_info) >= 3:
                return flight_info[1].strip()
        return 'None'

    @staticmethod
    def _parse_for_aircraft_type(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 5:
            flight_info = flight_info[-4][1:]  # Drop emoji in front
            flight_info = flight_info.split('|')
            if len(flight_info) >= 3:
                return flight_info[2].strip()
        return 'None'

    @staticmethod
    def _parse_for_departure(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-3][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 1:
                return flight_info[0].strip()
        return 'None'

    @staticmethod
    def _parse_for_departure_time(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-3][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 2:
                return flight_info[1].strip()
        return 'None'

    @staticmethod
    def _parse_for_arrival(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-2][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 1:
                return flight_info[0].strip()
        return 'None'

    @staticmethod
    def _parse_for_arrival_time(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-2][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 2:
                return flight_info[1].strip()
        return 'None'

    def parse_raw_tweets(self, tweets: Optional[List[Dict[str, Any]]] = None) -> pd.DataFrame:
        """
        Parse raw tweets from jsons
        """
        if tweets is None:
            tweets = self.etl()

        # Extract data from response object
        df = pd.DataFrame({
            'created_at': [pd.Timestamp(tweet['created_at']) for tweet in tweets],
            'tweet': [tweet['full_text'] for tweet in tweets],
            'user_mentions': [self._get_user_mentions(tweet) for tweet in tweets],
            'team_names': [self._get_team_names(tweet) for tweet in tweets],
            'flightware_links': [self._get_flightware_links(tweet) for tweet in tweets],
            'retweets': [tweet['retweet_count'] for tweet in tweets],
            'favorite_count': [tweet['favorite_count'] for tweet in tweets]
        })

        # Parse data from tweet
        df['tail_number'] = df['tweet'].apply(self._parse_for_tail_number)
        df['flight_no'] = df['tweet'].apply(self._parse_for_flight_no)
        df['aircraft_type'] = df['tweet'].apply(self._parse_for_aircraft_type)
        df['departure'] = df['tweet'].apply(self._parse_for_departure)
        df['departure_time'] = df['tweet'].apply(self._parse_for_departure_time)
        df['arrival'] = df['tweet'].apply(self._parse_for_arrival)
        df['arrival_time'] = df['tweet'].apply(self._parse_for_arrival_time)

        return df.drop_duplicates().reset_index(drop=True)
