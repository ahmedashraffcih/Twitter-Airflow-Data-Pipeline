import tweepy
import pandas as pd
import json
import s3fs
from datetime import datetime
from credentials import *


def run_twitter_etl():

    access_key = ACCESS_KEY
    access_secret = ACCESS_SECRET
    consumer_key = CONSUMER_KEY
    consumer_secret = CONSUMER_SECRET

    # Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # Creating An API Object
    api = tweepy.API(auth)

    # If you want to get information about specific user

    tweets = api.user_timeline(screen_name = '@elonmusk',
                                # 200 is the maximum allowed count
                                count = 200,
                                include_rts = False,
                                # Necessary to keep full_Text
                                # otherwise only the first 140 words are extracted
                                tweet_mode = "extended"
                                )

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]
        refined_tweet = {"user" : tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://twitter-airflow-bucket/elonmusk_twitter_data.csv")