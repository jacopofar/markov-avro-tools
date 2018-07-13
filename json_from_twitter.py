#!/usr/bin/env python3
# encoding: utf-8

# This script saves tweets in JSON instead of avro. Each line of the output file is a JSON-encoded tweet with metadata

import datetime
import json
import tweepy
import argparse
import os
import sys
import signal
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Twitter API credentials
consumer_key = config['twitter']['consumer_key']
consumer_secret = config['twitter']['consumer_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

writer=None

class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.counter = 0

    def on_status(self, status):
        self.counter += 1
        text = status.text
        if hasattr(status, 'retweeted_status'):
            text = status.retweeted_status.text
        author = status.author.screen_name
        print('{4} [{3}] {0} {1}: {2}'.format(status.id, author, text, self.counter, datetime.datetime.now().isoformat()))

        tags = list(map(lambda h: h['text'], status.entities['hashtags']))
        if status.user.location is not None:
            tags.append('loc:' + status.user.location.replace(',',''))
        tags.append('lang:' + status.lang)
        tags.append('src:' + status.source)
        writer.write(json.dumps({"text": text,
                       "source": 'twitter.com/{1}/status//{0}'.format(status.id, author),
                       "tags": tags,
                       "timestamp": status.created_at.isoformat()
                       }))
        writer.write('\n')


parser = argparse.ArgumentParser(description="store Tweets from a stream. it runs continuously until stopped")
parser.add_argument("-languages", type=str, help="the languages of the tweets, comma delimited", default=None)
parser.add_argument("-track", type=str, help="the keywords to search", default=None)

parser.add_argument("-output_file", type=str, help="the one-JSON-per-line serialized file", default="twitter_utterances.json")

args = parser.parse_args()

writer = open(args.output_file, "a+")

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)


track=args.track
if track is not None:
    track = track.split(',')
    print('will filter using the search keys: ' + ','.join(track))
else:
    print('will NOT filter on tweet content, no keywords were specified')

languages=args.languages
if languages is not None:
    languages = languages.split(',')
    print('will filter the languages: ' + ','.join(languages))
else:
    print('will NOT filter on tweet language, no languages were specified')


def signal_handler(signal, frame):
    print('\nSIGINT received, closing file and exiting...')
    writer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

myStream.filter(track=track, async=True, languages=languages)
