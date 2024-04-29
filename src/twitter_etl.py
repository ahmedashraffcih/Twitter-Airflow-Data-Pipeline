import tweepy
import pandas as pd
import json
import s3fs
import logging
from datetime import datetime
from infra.credentials import *


def run_twitter_etl():
    try:
# Twitter Authentication
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

        # Creating An API Object
        api = tweepy.API(auth)

        # If you want to get information about specific user

        # Fetch tweets
        tweets = api.user_timeline(screen_name=screen_name,
                                    count=200,
                                    include_rts=False,
                                    tweet_mode="extended")

        # Extract relevant tweet data
        tweet_list = []
        for tweet in tweets:
            text = tweet._json["full_text"]
            refined_tweet = {
                "user": tweet.user.screen_name,
                'text': text,
                'favorite_count': tweet.favorite_count,
                'retweet_count': tweet.retweet_count,
                'created_at': tweet.created_at
            }
            tweet_list.append(refined_tweet)

        # Convert to DataFrame
        df = pd.DataFrame(tweet_list)
        # Write to CSV in S3
        with s3fs.S3FileSystem() as s3:
            with s3.open(output_path, 'w') as file:
                df.to_csv(file, index=False)

        logging.info("Twitter ETL completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_twitter_etl(screen_name='@elonmusk', output_path='s3://twitter-airflow-bucket/elonmusk_twitter_data.csv')