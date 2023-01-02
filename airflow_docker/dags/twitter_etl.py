import tweepy
import pandas as pd
import json
from datetime import datetime
from airflow.hooks.S3_hook import S3Hook
import s3fs
import os

def run_twitter_etl():
        client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAB5SiAEAAAAAmdXX6Z8Ij6zu4Spk5N6d22KtRHA%3DZCz9QIjGupteEoacAoks7QLrAUekzBrMQJD1bmtNqKz1cRt21R')

        # Replace with your own search query
        query = 'from:elonmusk'
        # query = 'from:BillGates'
        # query = 'from:JamesMelville'


        tweets = tweepy.Paginator(client.search_recent_tweets, query=query,
                                    tweet_fields=['context_annotations', 'created_at','public_metrics'], max_results=100).flatten(limit=1000)

        tweet_lis = []
        

        lis = []
        for tweet in tweets:
            # print(tweet.text)
            lis.append(tweet.text)
            if len(tweet.context_annotations) > 0:
                domain_name_lis = []
                entity_name_lis = []

                for dic in tweet.context_annotations:
                    for key,value in dic.items():
                        if key == 'domain':
                            try:
                                domain_names = value['name']
                                domain_name_lis.append(domain_names)
                            except KeyError as e:
                                pass
                        elif key == 'entity':
                            try:
                                entity_names = value['name']
                                entity_name_lis.append(entity_names)
                                
                            except KeyError as e:
                                entity_description = None
                                pass
            refined_tweets = {'tweets':tweet.text,'created_at': tweet.created_at ,'reply_count':tweet.public_metrics["reply_count"],
                            'retweet_count': tweet.public_metrics["retweet_count"],
                            'like_count': tweet.public_metrics["like_count"],
                            'quote_count' : tweet.public_metrics["quote_count"],
                            'domain_names' : domain_name_lis, 
                            'entity_names' : entity_name_lis,}
            tweet_lis.append(refined_tweets)


        print(tweet_lis)
                
                
        df = pd.DataFrame(tweet_lis)
        df.to_csv("/opt/airflow/dags/elonmusk_tweets.csv")
        
        
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws-s3-conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    
    
def delete_from_s3(bucket_name: str,keys: str) -> None:
    hook = S3Hook('aws-s3-conn')
    hook.delete_objects(bucket=bucket_name, keys=keys)
    