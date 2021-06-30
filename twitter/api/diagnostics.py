import os
import pandas as pd

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from twitter.api.parser import Parser
from twitter.config import logger


class Diagnostics(Parser):
    """
    Create some quick plots
    """
    def diagnostics(self, df: pd.DataFrame):
        """
        Quick plots of the results
        """
        df['year'] = df['created_at'].dt.year
        df = df[(df['team_name'] != 'None') & (df['flightware_link'] != 'None')]

        logger.info('Diagnostics for Parsing')
        with PdfPages(os.path.join(self.save_dir, 'diagnostics.pdf')) as pdf:
            # Time series of tweets
            for parse_col in ['parsed', 'parsed_w_routing_no']:
                plt.figure(figsize=self.figsize)
                plt.title('Tweets over Time: {}'.format(parse_col))
                for parsed, df_plot in df.groupby(parse_col):
                    df_plot = df_plot.groupby('tweet_date').agg(num_tweets=('tweet', 'nunique')).reset_index()
                    df_plot = df_plot.sort_values('tweet_date').reset_index(drop=True)
                    plt.plot(
                        df_plot['tweet_date'],
                        df_plot['num_tweets'],
                        label='Parsed: {}'.format(parsed),
                        marker='o',
                        alpha=0.5
                    )
                plt.grid(True)
                plt.legend()
                plt.ylabel('Number of Tweets')
                plt.xlabel('Date')
                pdf.savefig()
                plt.close()

            for year, df_ in df.groupby('year'):
                df_plot = df_.groupby('team_name').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=True).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.barh(df_plot['team_name'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Top 50 Team Mentions in {}'.format(year))
                plt.ylabel('Team Name')
                plt.xlabel('Num tweets')
                pdf.savefig()
                plt.close()

                df_plot = df_.groupby('departure').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=True).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.barh(df_plot['departure'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Top 50 Departures in {}'.format(year))
                plt.ylabel('Departure Airport')
                plt.xlabel('Num tweets')
                pdf.savefig()
                plt.close()

                df_plot = df_.groupby('arrival').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=True).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.barh(df_plot['arrival'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Top 50 Arrivals in {}'.format(year))
                plt.ylabel('Arrival Airport')
                plt.xlabel('Num tweets')
                pdf.savefig()
                plt.close()

                df_plot = df_.groupby('aircraft_type').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=True).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.barh(df_plot['aircraft_type'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Top 50 Aircraft Types in {}'.format(year))
                plt.ylabel('Aircraft Type')
                plt.xlabel('Num tweets')
                pdf.savefig()
                plt.close()
