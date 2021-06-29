from setuptools import setup, find_packages

setup(
    name='twitter',
    version='1.0',
    description='Script to pull Tweets',
    author='Scott P. White',
    author_email='spwhite1337@gmail.com',
    packages=find_packages(),
    entry_points={'console_scripts': [
        'tw_pull = twitter.tweets:twitter_pull'
    ]},
    install_requires=[
        'pandas',
        'numpy',
        'openpyxl',
        'tqdm',
        'matplotlib',
        # Install tweepy from git to get premium api access
        # 'tweepy',
        'tweepy @ git+https://github.com/tweepy/tweepy@master',
    ],
)
