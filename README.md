# Marcus

To get the tweets you need to follow these steps

- I would recommend putting this in its own folder. For example, make a folder called `Tweets` in your `Documents` folder.

- Download `tweets.py` and `requirements.txt` files into some folder. I'll assume it is in `Documents/Tweets/` for now. 

- Open the terminal (spotlight search with `CMD + Space` and type `terminal`, hit enter)

- type `cd ~/Documents/Tweets`, hit Enter
    -- (if you save it into another folder, you need to type `cd ~/filepath` 
    where `filepath` is the path to the folder)
    
- type `pip install -r requirements.txt`. Some printouts for install progress will follow. 
If you get errors here let me know.

- type `python tweets.py --user SportsAviation --consumer_key key_from_email1 --consumer_secret key_from_email2
 --access_token_key key_from_email3 --access_token_secret key_from_email4` but with the strings I sent in the email 
 instead of `key_from_emailX`. I have to do this for security reasons, if I sent to you publicly Twitter will find out 
 and get mad.
 
 - For example, if `key_from_email1` was `abc`, `key_from_email2` was `def`, `key_from_email3` was `ghi`, 
 and `key_from_email4` was `jkl` then the final command would look like
 
 - `python tweets.py --user SportsAviation --consumer_key abc --consumer_secret def --access_token_key ghi --access_token_secret jkl`
 
 You should see some progress printouts and the spreadsheet saved as `SportsAviation.xlsx` in `Documents/Tweets`