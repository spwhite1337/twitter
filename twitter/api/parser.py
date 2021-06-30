import re
from typing import List, Dict, Any, Tuple, Optional
from tqdm import tqdm
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
            tweet_line_parts = [t.strip() for t in tweet_line.split('-')]
            # Should have two parts
            split_by_dash = len(tweet_line_parts) == 2
            if not split_by_dash:
                return split_by_dash

            # First part is airport code
            airport_code = tweet_line_parts[0]
            is_airport_code = bool(re.match('[A-Z]{3}', airport_code))
            # Second part is a time
            is_time = re.match(r'^[0-9]{1,2}:[0-9]{2}\s?[ap]m [A-Z]{2}', tweet_line_parts[1])
            return is_airport_code and is_time
        else:
            return False

    @staticmethod
    def _airport_format(version: str, ldx: int, tweet_line: str) -> Dict[str, str]:
        if version == 'v1':
            tweet_line_parts = [t.strip() for t in tweet_line.split('-')]
            return {
                '{}_airport_code'.format(ldx): tweet_line_parts[0],
                '{}_airport_time'.format(ldx): tweet_line_parts[1]
            }
        else:
            return {}

    @staticmethod
    def _is_aircraft_format(version: str, tweet_line: str) -> bool:
        if version == 'v1':
            tweet_line_parts = [t.strip() for t in tweet_line.split('|')]
            # Should have two or 3 parts
            split_by_pipe = len(tweet_line_parts) in [2, 3]
            if not split_by_pipe:
                return split_by_pipe

            # No line part should be more than 7 characters or less than 2
            for tweet_line_part in tweet_line_parts:
                if len(tweet_line_part) > 7 or len(tweet_line_part) < 2:
                    return False
            return True

    @staticmethod
    def _aircraft_format(version: str, tweet_line: str) -> Dict[str, str]:
        if version == 'v1':
            tweet_line_parts = [t.strip() for t in tweet_line.split('|')]
            tail_no, routing_no, aircraft_type = None, None, None
            for idx, tweet_line_part in enumerate(tweet_line_parts):
                # Capture the international tail-nos
                if tweet_line_part in [
                    'TF-FIO', 'CGBIK', 'CGJVX', 'F-HNCO', 'CFYKR', 'C-GBIK', 'C-GBHN', 'C-GJWI', 'C-FYKR',
                    'C-GBIA', 'C-FGJI',
                ]:
                    tail_no = tweet_line_part
                    continue
                # Capture the canadian ones
                if idx in [0, 1] and re.match('^C-?[A-Z]{4}', tweet_line_part):
                    tail_no = tweet_line_part
                    continue
                # Capture the mexican ones
                if idx == 0 and re.match('^XA-[A-Z]{3}', tweet_line_part):
                    tail_no = tweet_line_part
                    continue
                # If it starts with N and has at least one number, and more than 3 characters it is a tail_no
                # https://en.wikipedia.org/wiki/Aircraft_registration
                if tweet_line_part.startswith('N') and \
                        re.match('.*\d.*', tweet_line_part) and \
                        len(tweet_line_part) > 3:
                    tail_no = tweet_line_part
                    continue

                # If it starts with a capital letter, has 2, 3, or 4 characters -> Then it is an aircraft-type
                # https://en.wikipedia.org/wiki/List_of_aircraft_type_designators
                elif re.match('^[A-Z]', tweet_line_part) and len(tweet_line_part) in [2, 3, 4]:
                    aircraft_type = tweet_line_part
                    continue

                # If it starts with 2 or 3 Capital letters then 1-4 digits then it is a routing number
                elif re.match('^[A-Z]{2,3}[0-9]{1,4}', tweet_line_part):
                    routing_no = tweet_line_part
                    continue

            return {
                'tail_no': tail_no,
                'aircraft_type': aircraft_type,
                'routing_no': routing_no
            }
        else:
            return {}

    @staticmethod
    def _is_layover_format(version: str, tweet_line: str) -> bool:
        if version == 'v1':
            # If the pipe operate splits it into two and the first part is split into two by a dash then it is a layover
            if len(tweet_line.split('|')) == 2 and len(tweet_line.split('|')[0].split('-')) == 2:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def _layover_format(version: str, ldx: int, tweet_line: str) -> Dict[str, str]:
        if version == 'v1':
            tweet_line_parts = [t.strip() for t in tweet_line.split('|')[0].split('-')]
            return {
                '{}_airport_code'.format(ldx): tweet_line_parts[0],
                '{}_airport_time'.format(ldx): tweet_line_parts[1]
            }
        else:
            return {}

    def _parse_tweet(self, version: str, tweet: str) -> Dict[str, str]:
        """
        Parse a tweet based on observed logic
        """
        result = {}
        if version == 'v1':
            # Strip emojis
            tweet = '\n'.join([self.remove_emojis(tweet_line) for tweet_line in tweet.split('\n')])
            # iterate over the lines in the tweet
            for ldx, tweet_line in enumerate(tweet.split('\n')):
                # Check for a layover line
                if self._is_layover_format(version, tweet_line):
                    result.update(self._layover_format(version, ldx, tweet_line))
                    continue

                # Airport code + arrival / layover / departure time
                if self._is_airport_format(version, tweet_line):
                    result.update(self._airport_format(version, ldx, tweet_line))
                    continue

                # Aircraft line for routing_no, aircraft_type, and tail_no
                if self._is_aircraft_format(version, tweet_line):
                    result.update(self._aircraft_format(version, tweet_line))
                    continue

            # Convert the "first" airport code / time to "Departure" and last to "arrival"
            if len(result) > 0:
                airport_codes = [int(a[0]) for a in result.keys() if 'airport_code' in a]
                if len(airport_codes) == 0:
                    return result
                min_code, max_code = min(airport_codes), max(airport_codes)
                result['departure'] = result[str(min_code) + '_airport_code']
                result['departure_time'] = result[str(min_code) + '_airport_time']
                result['arrival'] = result[str(max_code) + '_airport_code']
                result['arrival_time'] = result[str(max_code) + '_airport_time']

                # Add a layover if there are 3 entries for airport-codes / times
                if max_code - min_code == 3:
                    result['layover'] = result['{}_airport_code'.format(max_code - 1)]
                    result['layover_time'] = result['{}_airport_time'.format(max_code - 1)]

                # Drop intermediate codes
                drops = []
                for key in result.keys():
                    if 'airport_code' in key or 'airport_time' in key:
                        drops.append(key)
                for drop in drops:
                    result.pop(drop)
            return result

        else:
            return {}

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
        }).drop_duplicates()

        # Define parsing versions
        df['tweet_date'] = df['created_at'].dt.date
        df['p_version'] = df['created_at'].apply(self._parsing_versions)

        # Parse data from tweet
        df['is_retweet'] = df['tweet'].apply(lambda t: t.startswith('RT @'))

        # Filter for only posts
        df_posts = df[~df['is_reply'] & ~df['is_quote_status'] & ~df['is_retweet']]

        # Iterate over versions / tweets
        records = []
        for p_version, df_ in df_posts.groupby('p_version'):
            logger.info('Parsing {}'.format(p_version))
            for _, row in tqdm(df_.iterrows(), total=df_.shape[0]):
                record = {
                    'created_at': row['created_at'],
                    'tweet_date': row['tweet_date'],
                    'tweet': row['tweet'],
                    'user_mention': row['user_mentions'],
                    'team_name': row['team_names'],
                    'flightware_link': row['flightware_links'],
                    'p_version': p_version
                }
                results = self._parse_tweet(p_version, row['tweet'])
                record.update(results)
                records.append(record)
        df = pd.DataFrame.from_records(records).drop_duplicates().reset_index(drop=True)

        # If it is fully parsed it should have a team-name, link, tail_no, aircraft_type, departure, arrival
        df['parsed'] = ~df[[
            'team_name',
            'flightware_link',
            'aircraft_type',
            'tail_no',
            'departure',
            'arrival',
            'departure_time',
            'arrival_time'
        ]].isna().any(axis=1)
        return df
