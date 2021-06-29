
# Requirements

- Python 3.6
- Developer Credentials fro Twitter


# Procedure

- `pip install -e .`
- `consumer_key=API KEY from twitter`
- `consumer_secret=API Secret Key from twitter`
- `access_token_key=Access Token from twitter`
- `access_token_secret=Access Token Secret from twitter`


# Marcus

To get the tweets you need to follow these steps

- 1.) In the top right of this page, click the green `Clone or Download` button, click `Download Zip`. 
I'll assume it is in your `Downloads` folder for now.

- 2.) Open the terminal (spotlight search with `CMD + Space` and type `terminal`, hit enter)

- 3.) type `cd ~/Downloads/twitter-master`, hit Enter
    -- (if you save it into another folder, you need to type `cd ~/filepath` 
    where `filepath` is the path to the folder)
    
- 4.) type `pip install -r requirements.txt`, hit Enter. Some printouts for install progress will follow. 
If you get errors here let me know.

- 5.) type `python tweets.py --user SportsAviation --consumer_key key_from_email1 --consumer_secret key_from_email2
 --access_token_key key_from_email3 --access_token_secret key_from_email4` but with the strings I sent in the email 
 instead of `key_from_emailX` and hit Enter. I have to pass the keys over email this for security reasons, 
 if I sent to you publicly Twitter will find out 
 and get mad. I will send you a command in an email you can copy/paste instead.
  
 You should see some progress printouts and the spreadsheet saved as `SportsAviation.xlsx` in `Documents/twitter-master`