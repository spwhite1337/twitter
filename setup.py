from setuptools import setup, find_packages

setup(
    name='twitter',
    version='1.0',
    description='Script to pull Tweets',
    author='Scott P. White',
    author_email='spwhite1337@gmail.com',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'tweepy',
        'openpyxl',
        'tqdm'
    ],
)
