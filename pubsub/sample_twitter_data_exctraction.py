from google.cloud import pubsub
import tweepy
from tweepy import StreamListener
import twitter_setting as auths
import json
import base64

def get_tweets():

    auth = tweepy.OAuthHandler(auths.twitter_api_key,auths.twitter_api_secret_key)
    auth.set_access_token(auths.twitter_access_token,auths.twitter_secret_key)
    api = tweepy.API(auth)
    no = 200
    tweets = api.search(q='Sachin', language='en')
    temp =[]
    tweet_csv = [tweet.text for tweet in tweets]
    print(tweets)
    #strem = base64.urlsafe_b64decode(tweets)
    # json_load = json.loads(tweets)
    # print(json_load)

    # for tweet in tweets:
    #    print(tweet.id,tweet.user.screen_name,tweet.text,tweet.created_at,tweet.source,tweet.retweeted)
    #     #print(tweet)
    # #print(temp)

get_tweets()



