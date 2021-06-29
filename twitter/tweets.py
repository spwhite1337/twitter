import argparse

from twitter.api import TwitterPull


def twitter_pull():
    parser = argparse.ArgumentParser()
    parser.add_argument('--screen_name', type=str, required=True)
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()

    api = TwitterPull(screen_name=args.screen_name, overwrite=args.overwrite)
    api.etl()
