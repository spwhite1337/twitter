import argparse

from twitter.api import TwitterPull


def twitter_pull():
    parser = argparse.ArgumentParser()
    parser.add_argument('--screen_name', type=str, required=True)
    parser.add_argument('--to_date', type=str, required=False, default=None)
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()

    api = TwitterPull(screen_name=args.screen_name)
    tweets = api.etl(to_date=args.to_date, overwrite=args.overwrite)
    df = api.parse_raw_tweets(tweets)
    api.save_parsed(df)
    api.diagnostics(df)
