from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import MySQLdb
import time
import json
import re
import preprocessor as clean_tweet

# Setting MySQL 
conn = MySQLdb.connect("localhost","root","","divernity")

c = conn.cursor()
conn.set_character_set('utf8mb4')
c.execute('SET NAMES utf8mb4;')
c.execute('SET CHARACTER SET utf8mb4;')
c.execute('SET character_set_connection=utf8mb4;')

#consumer key, consumer secret, access token, access secret.
ckey="CONSUMER KEY"
csecret="CONSUMER SECRET"
atoken="ACCESS TOKEN"
asecret="ACCESS SECRET"

clean_tweet.set_options(clean_tweet.OPT.URL, clean_tweet.OPT.EMOJI,clean_tweet.OPT.MENTION,clean_tweet.OPT.HASHTAG,clean_tweet.OPT.NUMBER,clean_tweet.OPT.SMILEY)
class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)
        tweet = clean_tweet.clean(self.remove_emoticon(all_data["text"]))
        username = all_data["user"]["screen_name"]
        c.execute("INSERT INTO dataset(username,text,sumber) VALUES (%s,%s,'twitter')",
                 (username, tweet))
        conn.commit()
        print((username,tweet)) 
        return(True)

    def on_error(self, status):
        print(status)

    def remove_emoticon(self,text):
        emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]+", flags=re.UNICODE)

        return emoji_pattern.sub(r'', text)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["jokowi","prabowo","pilpres","sandiaga","maruf amin","pdip","gerindra"])



