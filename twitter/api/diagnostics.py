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
        df = df[(df['team_names'] != 'None') & (df['departure'] != 'None') & (df['arrival'] != 'None')].copy()
        df['dt'] = df['created_at'].dt.date
        df['year'] = df['created_at'].dt.year

        logger.info('Diagnostics for Parsing')
        with PdfPages(os.path.join(self.save_dir, 'diagnostics.pdf')) as pdf:
            # Time series of tweets
            df_plot = df.groupby('dt').agg(num_tweets=('tweet', 'nunique')).reset_index()
            df_plot = df_plot.sort_values('dt').reset_index(drop=True)
            plt.figure(figsize=self.figsize)
            plt.title('Tweets over Time')
            plt.plot(df_plot['dt'], df_plot['num_tweets'])
            plt.grid(True)
            plt.ylabel('Number of Tweets')
            plt.xlabel('Date')
            pdf.savefig()
            plt.close()

            for year, df_ in df.groupby('year'):
                df_plot = df_.groupby('team_names').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=False).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.bar(df_plot['team_names'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Team Mentions in {}'.format(year))
                plt.xlabel('Team Name')
                plt.ylabel('Num tweets')
                pdf.savefig()
                plt.close()

                df_plot = df_.groupby('departure').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=False).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.bar(df_plot['departure'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Departures in {}'.format(year))
                plt.xlabel('Departure Airport')
                plt.ylabel('Num tweets')
                pdf.savefig()
                plt.close()

                df_plot = df_.groupby('arrival').agg(tweet=('created_at', 'nunique')).reset_index()
                df_plot = df_plot.sort_values('tweet', ascending=False).reset_index().head(50)
                plt.figure(figsize=self.figsize)
                plt.bar(df_plot['arrival'], df_plot['tweet'])
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.title('Arrivals in {}'.format(year))
                plt.xlabel('Arrival Airport')
                plt.ylabel('Num tweets')
                pdf.savefig()
                plt.close()
