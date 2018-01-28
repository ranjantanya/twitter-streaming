import json
import threading
import time

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from stop_words import get_stop_words
from textblob import TextBlob
import re

stop_words = get_stop_words('english')
stop_words.append(get_stop_words('en'))
# print(get_stop_words('en'))
# print(stop_words)

count_of_user_tweets = dict()
count_of_unique_words = dict()

import local_config as config

lst = []


class TweetProcessor(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        counter = 1
        while True:
            time.sleep(60)
            current_millis = int(round(time.time() * 1000))
            if counter >= 6:
                self.delete_old_tweets(current_millis)
            counter = counter + 1

            # generate reports based on last five mins data
            self.generate_reports(current_millis)

    def delete_old_tweets(self, current_millis):
        global lst
        print("Tweet list length before deleting old tweets {}".format(str(len(lst))))
        first_min_end_ts = current_millis - 300000 # current time - 5 mins
        # delete the 1st minute data
        index = 0
        for tweet in lst:
            tweet_timestamp = int(tweet['timestamp_ms'])
            if tweet_timestamp >= first_min_end_ts:
                break
            else:
                index = index + 1

        # slice tweet list
        lst = lst[index:]
        print("Tweet list length after deleting old tweets {}".format(str(len(lst))))

    def generate_user_report(self, tweets_in_last_5_min_window):
        if tweets_in_last_5_min_window is not None and len(tweets_in_last_5_min_window) != 0:
            global count_of_user_tweets, count_of_unique_words
            count_of_unique_words.clear()
            count_of_user_tweets.clear()
            try:
                for tweet in tweets_in_last_5_min_window:
                    user_name = tweet['user']['name']

                    count_user_tweets = count_of_user_tweets.get(user_name, 0) + 1
                    count_of_user_tweets[user_name] = count_user_tweets
                    if 'extended_tweet' in tweet.keys():
                        tweet_text = tweet['extended_tweet'][
                            'full_text']  # text contains truncated tweet, so pick from extended_tweet
                    else:
                        tweet_text = tweet['text']
                    tweet_text = re.sub(r'@\w+', '', tweet_text)  # remove @ mentions
                    tweet_blob = TextBlob(tweet_text.lower())  # tokenize the tweet into words without punctuations

                    for word in tweet_blob.words:
                        if word not in stop_words and word != "'s" and len(word) > 1:
                            count_of_word = count_of_unique_words.get(word, 0) + 1
                            count_of_unique_words[word] = count_of_word

            except:
                print(tweet)

        if count_of_user_tweets is None or len(count_of_user_tweets) == 0:
            print('zero users have tweeted')
        else:
            for user in count_of_user_tweets:
                # pass
                print('{user} has posted {count} tweets'.format(user=user, count=count_of_user_tweets[user]))

    def generate_links_report(self, tweets_in_last_5_min_window):
        pass

    def generate_content_report(self, tweets_in_last_5_min_window):
        global count_of_unique_words
        if count_of_unique_words is None or len(count_of_unique_words) == 0:
            # pass
            print('zero words till now')

        else:
            print('\nNo. of unique words till now: {count}'.format(count=len(count_of_unique_words.keys())))
            sorted_word_count = [k for k in sorted(count_of_unique_words, key=count_of_unique_words.get, reverse=True)]
            print('\nThe most frequently used 10 words are:')
            for i in range(0, 10):
                # pass
                print(sorted_word_count[i])

    def generate_reports(self, current_millis):
        global lst
        tweets_in_last_5_min_window = []
        for tweet in lst:
            tweet_timestamp = int(tweet['timestamp_ms'])
            if tweet_timestamp <= current_millis:
                tweets_in_last_5_min_window.append(tweet)

        self.generate_user_report(tweets_in_last_5_min_window)
        self.generate_content_report(tweets_in_last_5_min_window)
        self.generate_links_report(tweets_in_last_5_min_window)


def timer():
    now = time.localtime(time.time())
    return now[5]


class TwitterStreamListener(StreamListener):
    def on_data(self, data):
        global lst
        json_data = json.loads(data)
        lst.append(json_data)
        return True

    def on_error(self, status):
        if status == 420:
            print('rate limit reached')
            return False


if __name__ == "__main__":
    l = TwitterStreamListener()
    secrets = config.my_twitter_keys()
    consumer_key = secrets['consumer_key']
    consumer_secret = secrets['consumer_secret']
    token_key = secrets['token_key']
    token_secret = secrets['token_secret']
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(token_key, token_secret)
    keyword = None
    while keyword is None:
        keyword = input('enter the keyword: ')
        if keyword == ' ' or keyword == '':
            keyword = None

    stream = Stream(auth, l)

    stream.filter(track=[keyword], async=True)
    thread = TweetProcessor(1, 'ReportThread')
    thread.start()
