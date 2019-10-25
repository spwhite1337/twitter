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

    # Save jsons
    with open(f'{screen_name}_tweets.pkl', 'wb') as fp:
        pickle.dump(tweet_jsons, fp)

    # Get team as first user mention
    user_mentions, team_names = [], []
    for tweet in tweet_jsons:
        mentions = tweet.get('entities', []).get('user_mentions', [])
        if len(mentions) > 0:
            user_mentions.append(mentions[0].get('screen_name', 'None'))
            team_names.append(mentions[0].get('name', 'None'))
        else:
            user_mentions.append('None')
            team_names.append('None')

    # Get departures, arrivals, flight nos, tail nos, aircrafts by parsing tweet
    departures, arrivals = [], []
    flight_nos, tail_nos, aircrafts = [], [], []
    for tweet in tweet_jsons:
        split = tweet['full_text'].split('\n')
        if len(split) > 1:
            departures.append(split[1][:3])
            arrivals.append(split[1][-3:])
        else:
            departures.append('None')
            arrivals.append('None')

        if len(split) > 2:
            pipe_split = split[2].split('|')
            if len(pipe_split) > 1:
                flight_nos.append(pipe_split[0])
                tail_nos.append(pipe_split[1])
            else:
                flight_nos.append('None')
                tail_nos.append('None')
            if len(pipe_split) > 2:
                aircrafts.append(pipe_split[2])
            else:
                aircrafts.append('None')
        else:
            flight_nos.append('None')
            tail_nos.append('None')
            aircrafts.append('None')

    # Get first link
    flightware_links = []
    for tweet in tweet_jsons:
        urls = tweet.get('entities', []).get('urls', [])
        if len(urls) > 0:
            flightware_links.append(urls[0].get('url', 'None'))
        else:
            flightware_links.append('None')

    # Save as xlsx
    df_tweets = pd.DataFrame()
    df_tweets['created_at'] = [tweet['created_at'] for tweet in tweet_jsons]
    df_tweets['tweet_date'] = pd.to_datetime(df_tweets['created_at']).dt.date
    df_tweets['text'] = [tweet['full_text'] for tweet in tweet_jsons]
    df_tweets['first_user_mention'] = user_mentions
    df_tweets['team_name'] = team_names
    df_tweets['departures'] = departures
    df_tweets['arrivals'] = arrivals
    df_tweets['flight_nos'] = flight_nos
    df_tweets['tail_nos'] = tail_nos
    df_tweets['aircrafts'] = aircrafts
    df_tweets['flightware_links'] = flightware_links
    df_tweets['retweets'] = [tweet['retweet_count'] for tweet in tweet_jsons]
    df_tweets['favorite_count'] = [tweet['favorite_count'] for tweet in tweet_jsons]

    # Save to excel
    df_tweets.to_excel(f'{screen_name}_tweets.xlsx', index=False)


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
