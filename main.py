import json
import re
import signal
import sys
import threading
import time

from stop_words import get_stop_words
from textblob import TextBlob
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import config

stop_words = get_stop_words('english')
stop_words.append(get_stop_words('en'))

user_to_tweet_count_map = dict()
word_to_count_map = dict()
domain_name_to_count_map = dict()
total_count_of_urls = 0
tweet_list = []
rate_limit_reached = False
FIVE_MINUTES_IN_MS = 300000
WAIT_TIME_BEFORE_NEXT_PROCESSING = 60
DOMAIN_NAME_PATTERN = '\/\/([a-zA-Z0-9.-]*)[\/\?]?'
start_time = None
stream = None


def signal_handler(signal, frame):
    global stream
    print('Keyboard Interrupt! Exiting...')
    if stream is not None:
        stream.disconnect()
        print("Stream disconnected")
        stream = None
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


class TweetProcessor(threading.Thread):
    """
    Handles heavy lifting task of processing tweets and generating reports.
    """

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        global start_time, tweet_list, rate_limit_reached
        current_millis = int(round(time.time() * 1000))

        # if more than 5 mins have elapsed then start deleting tweets which are older
        # than 5 mins. This way we will maintain a window of latest tweets only (last 5 mins only)
        if current_millis - FIVE_MINUTES_IN_MS > start_time:
            self.delete_old_tweets(current_millis)

        print('########################################## Report Start ##########################################')
        if len(tweet_list) > 0:
            self.preprocess_tweets(current_millis)
            self.generate_user_report()
            # self.generate_content_report()
            # self.generate_links_report()
        else:
            if rate_limit_reached:
                print('Rate limit has reached. Deferring report generation.')
            else:
                print('No tweets found')
        print('########################################## Report End ##########################################')

    def delete_old_tweets(self, current_millis):
        """
        Deletes tweets which are older than 5 mins

        :param current_millis: timestamp when current report generation started
        :return:
        """
        global tweet_list
        # print('\nTweet list length before deleting old tweets {}'.format(str(len(tweet_list))))
        first_min_end_ts = current_millis - FIVE_MINUTES_IN_MS  # current time - 5 mins
        # delete the 1st minute data
        index = 0
        for tweet in tweet_list:
            tweet_timestamp = int(tweet['timestamp_ms'])
            if tweet_timestamp >= first_min_end_ts:
                break
            else:
                index = index + 1

        # slice tweet list to remove older tweets
        tweet_list = tweet_list[index:]
        # print('Number of tweets deleted {}'.format(index))
        # print('Tweet list length after deleting old tweets {}'.format(len(tweet_list)))

    def preprocess_tweets(self, current_millis):
        """
        Preprocess tweets, within the last 5 mins, to populate global dictionaries with
        details of users, words, links which will be used for report generation

        :param current_millis: timestamp when the report generation started
        :return:
        """
        global tweet_list, user_to_tweet_count_map, word_to_count_map, domain_name_to_count_map, total_count_of_urls
        # clear global dictionaries to start afresh
        domain_name_to_count_map.clear()
        user_to_tweet_count_map.clear()
        word_to_count_map.clear()
        total_count_of_urls = 0

        # filter to fetch the latest tweets (within last 5 mins)
        # needed because new tweets might get added while we're processing
        tweets_in_last_5_min_window = []
        for tweet in tweet_list:
            try:
                tweet_timestamp = int(tweet['timestamp_ms'])
                # ignore older tweets (more than 5 mins old)
                # ignore tweets which are newer than when the report generation started
                # ignore retweets
                if (current_millis - FIVE_MINUTES_IN_MS) <= tweet_timestamp <= current_millis:
                    tweets_in_last_5_min_window.append(tweet)
            except Exception as e:
                print('Error while processing tweet: {}'.format(str(tweet)))
                print('Exception {}'.format(e))

        print("Relevant tweet list size {}".format(len(tweets_in_last_5_min_window)))

        # process relevant tweets and fill up global dictionaries
        for tweet in tweets_in_last_5_min_window:
            try:
                # process users
                user_id = tweet['user']['id']
                user_name = tweet['user']['name']
                count_user_tweets = user_to_tweet_count_map.get((user_id, user_name), 0) + 1
                user_to_tweet_count_map[(user_id, user_name)] = count_user_tweets

                # process urls
                if 'extended_tweet' in tweet.keys():
                    # text contains truncated tweet, so pick from extended_tweet
                    list_of_urls = tweet['extended_tweet']['entities']['urls']
                    tweet_text = tweet['extended_tweet']['full_text']
                else:
                    list_of_urls = tweet['entities']['urls']
                    tweet_text = tweet['text']

                if list_of_urls is not None and len(list_of_urls) != 0:
                    for url_in_tweet in list_of_urls:
                        total_count_of_urls += 1
                        expanded_url = url_in_tweet['expanded_url']
                        domain = re.search(DOMAIN_NAME_PATTERN, expanded_url)
                        if domain:
                            domain_name = domain.group(1)
                            url_count = domain_name_to_count_map.get(domain_name, 0) + 1
                            domain_name_to_count_map[domain_name] = url_count

                # process words
                tweet_text = re.sub(r'@\w+', '', tweet_text)  # remove @ mentions
                tweet_blob = TextBlob(tweet_text.lower())  # tokenize the tweet into words without punctuations

                for word in tweet_blob.words:
                    if word not in stop_words and word != "'s" and len(word) > 1:
                        count_of_word = word_to_count_map.get(word, 0) + 1
                        word_to_count_map[word] = count_of_word

            except Exception as e:
                print('Error in tweet: {}'.format(str(tweet)))
                print('Exception {}'.format(e))

    def generate_user_report(self):
        """
        Prints the names of all Twitter users who tweeted with given keyword along with the count of tweets from them.
        """
        print('************** User Report **************')
        global user_to_tweet_count_map
        if user_to_tweet_count_map is None or len(user_to_tweet_count_map) == 0:
            print('Zero users have tweeted with the given keyword')
        else:
            for (user_id, user_name) in user_to_tweet_count_map:
                print('{user} has posted {count} tweets'.format(user=user_name,
                                                                count=user_to_tweet_count_map[(user_id, user_name)]))

    def generate_links_report(self):
        """"
        Prints the total number of links included in the tweets followed by a list of the unique domains sorted
        by the count. Any shortened links are expanded
        """
        print('************** Links Report **************')
        global domain_name_to_count_map, total_count_of_urls

        print('Total number of links in the tweets = {}'.format(total_count_of_urls))
        if domain_name_to_count_map is None or len(domain_name_to_count_map) == 0:
            print('Zero urls till now')
        else:
            print('List of unique domains in decreasing frequency:')
            sorted_domain_with_frequency = [k for k in
                                            sorted(domain_name_to_count_map, key=domain_name_to_count_map.get,
                                                   reverse=True)]
            for domain in sorted_domain_with_frequency:
                print('{domain}    :   {count}'.format(domain=domain, count=domain_name_to_count_map[domain]))

    def generate_content_report(self):
        """
        Prints the number of unique words used in the tweets followed by the list of top 10 words sorted by occurence.
        Removes the common function words like a, an, the, of, with etc.
        """
        print('************** Content Report **************')
        global word_to_count_map
        if word_to_count_map is None or len(word_to_count_map) == 0:
            print('Zero words till now')

        else:
            print('No. of unique words till now: {count}'.format(count=len(word_to_count_map.keys())))
            sorted_word_count = [k for k in sorted(word_to_count_map, key=word_to_count_map.get, reverse=True)]
            print('The most frequently used 10 words are:')
            for i in range(0, 10):
                # pass
                print(sorted_word_count[i])


class TweetTimer(threading.Thread):
    """
    Thread which generates report every 1 minute.
    It calls a new thread for carrying out report generation which will do the
    processing and thereby maintaining correct timer.
    """

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        global stream
        while stream is not None:
            # wait before generating the report
            time.sleep(WAIT_TIME_BEFORE_NEXT_PROCESSING)
            # generate report in a new thread
            TweetProcessor(2, "Tweet Reporter").start()

        print("Exiting Timer")

class TwitterStreamListener(StreamListener):
    """
    Custom listener which processes each tweet received via the Twitter Streaming API
    """

    def on_data(self, data):
        """
        Called when raw data is received from the connection

        :param data: raw tweet data received
        :return:
        """
        global tweet_list
        tweet_json = json.loads(data)
        # adding only valid tweets and ignoring retweets
        if 'timestamp_ms' in tweet_json and 'retweeted_status' not in tweet_json.keys():
            tweet_list.append(tweet_json)
        return True

    def on_error(self, status):
        """
        Called when a non-200 status code is returned

        :param status: response status code
        :return:
        """
        global rate_limit_reached
        if status == 420:
            rate_limit_reached = True
            print('Rate limit reached. Retrying in some time.')
        return True

    def on_connect(self):
        """"
        Called once connected to streaming server.

        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        global rate_limit_reached
        rate_limit_reached = False
        print('Starting report generation')


if __name__ == "__main__":
    # Fetch api keys from config file
    secrets = config.my_twitter_keys()
    consumer_key = secrets['consumer_key']
    consumer_secret = secrets['consumer_secret']
    token_key = secrets['token_key']
    token_secret = secrets['token_secret']

    # instantiate oauth handler
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(token_key, token_secret)

    # ask for keyword to track
    keyword = None
    while keyword is None:
        keyword = input('Enter the keyword to track: ')
        keyword = keyword.strip()
        if keyword == '':
            print('Please enter a valid keyword\n')
            keyword = None

    # track start time. Useful for processing the first 5 minutes of data
    start_time = int(round(time.time() * 1000))

    # initialize stream listener and start accepting tweets via twitter streams API
    stream_listener = TwitterStreamListener()
    stream = Stream(auth, stream_listener)

    # start a new thread to generate reports periodically
    TweetTimer(1, 'Tweet Timer').start()

    # start tracking the keyword
    stream.filter(track=[keyword])
