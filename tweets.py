import tweepy
import pickle
import argparse
import pandas as pd


def get_all_tweets(screen_name, keys):
    # Twitter only allows access to a users most recent 3240 tweets with this method
    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(keys['consumer_key'], keys['consumer_secret'])
    auth.set_access_token(keys['access_token_key'], keys['access_token_secret'])
    api = tweepy.API(auth)
    # initialize a list to hold all the tweepy Tweets
    alltweets = []
    # make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name=screen_name, count=200, tweet_mode='extended')
    # save most recent tweets
    alltweets.extend(new_tweets)
    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print(f"getting tweets before {oldest}")
        # all subsequent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest, tweet_mode='extended')
        # save most recent tweets
        alltweets.extend(new_tweets)
        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        print(f"...{len(alltweets)} tweets downloaded so far")

    # Convert to list of Jsons
    tweet_jsons = [tweet._json for tweet in alltweets]
    # Get team as first user mention, team-name as name of twitter account for first user-mention
    user_mentions, team_names = [], []
    for tweet in tweet_jsons:
        mentions = tweet.get('entities', []).get('user_mentions', [])
        if len(mentions) > 0:
            user_mentions.append(mentions[0].get('screen_name', 'None'))
            team_names.append(mentions[0].get('name', 'None'))
        else:
            user_mentions.append('None')
            team_names.append('None')

    def get_tail_number(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 5:
            flight_info = flight_info[-4][1:]  # Drop emoji in front
            flight_info = flight_info.split('|')
            if len(flight_info) >= 3:
                return flight_info[0].strip()[1:]
        return 'None'

    def get_flight_no(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 5:
            flight_info = flight_info[-4][1:]  # Drop emoji in front
            flight_info = flight_info.split('|')
            if len(flight_info) >= 3:
                return flight_info[1].strip()
        return 'None'

    def get_aircraft_type(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 5:
            flight_info = flight_info[-4][1:]  # Drop emoji in front
            flight_info = flight_info.split('|')
            if len(flight_info) >= 3:
                return flight_info[2].strip()
        return 'None'

    def get_departure(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-3][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 1:
                return flight_info[0].strip()
        return 'None'

    def get_departure_time(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-3][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 2:
                return flight_info[1].strip()
        return 'None'

    def get_arrival(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-2][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 1:
                return flight_info[0].strip()
        return 'None'

    def get_arrival_time(tweet: str) -> str:
        flight_info = tweet.split('\n')
        if len(flight_info) >= 3:
            flight_info = flight_info[-2][1:]  # Drop emoji in front
            flight_info = flight_info.split(' - ')
            if len(flight_info) >= 2:
                return flight_info[1].strip()
        return 'None'

    # Get first link for flightware
    flightware_links = []
    for tweet in tweet_jsons:
        urls = tweet.get('entities', []).get('urls', [])
        if len(urls) > 0:
            flightware_links.append(urls[0].get('url', 'None'))
        else:
            flightware_links.append('None')

    # Save as xlsx
    df = pd.DataFrame()
    df['created_at'] = [tweet['created_at'] for tweet in tweet_jsons]
    df['tweet_date'] = pd.to_datetime(df['created_at']).dt.date
    df['text'] = [tweet['full_text'] for tweet in tweet_jsons]
    df['first_user_mention'] = user_mentions
    df['team_name'] = team_names

    df['aircraft_type'] = df['text'].apply(get_aircraft_type)
    df['tail_number'] = df['text'].apply(get_tail_number)
    df['flight_no'] = df['text'].apply(get_flight_no)
    df['departure'] = df['text'].apply(get_departure)
    df['arrival'] = df['text'].apply(get_arrival)
    df['departure_time'] = df['text'].apply(get_departure_time)
    df['arrival_time'] = df['text'].apply(get_arrival_time)

    df['flightware_links'] = flightware_links
    df['retweets'] = [tweet['retweet_count'] for tweet in tweet_jsons]
    df['favorite_count'] = [tweet['favorite_count'] for tweet in tweet_jsons]

    # Save to excel
    df = df[df['aircraft_type'] != 'None']
    df.to_csv(f'{screen_name}_tweets.csv', index=False)
    df.to_excel(f'{screen_name}_tweets.xlsx', index=False)


if __name__ == '__main__':
    # Arguments include the user you want to download and the api keys
    parser = argparse.ArgumentParser(description='Get Twitter data')
    parser.add_argument('--user', action='store', default='none', type=str, )
    parser.add_argument('--consumer_key', action='store', default='none', type=str, )
    parser.add_argument('--consumer_secret', action='store', default='none', type=str, )
    parser.add_argument('--access_token_key', action='store', default='none', type=str, )
    parser.add_argument('--access_token_secret', action='store', default='none', type=str, )
    args = parser.parse_args()
    # API Keys
    twitter_keys = {
        'consumer_key': args.consumer_key,
        'consumer_secret': args.consumer_secret,
        'access_token_key': args.access_token_key,
        'access_token_secret': args.access_token_secret
    }

    get_all_tweets(args.user, twitter_keys)
