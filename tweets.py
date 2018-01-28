import json
import threading
import time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from stop_words import get_stop_words
from textblob import TextBlob
import re
import local_config as config

stop_words = get_stop_words('english')
stop_words.append(get_stop_words('en'))

count_of_user_tweets = dict()
count_of_unique_words = dict()
count_of_each_domain_url = dict()
total_count_of_urls = 0
tweet_list = []
rate_limit_reached = False
start_time = None
FIVE_MINUTES_IN_MS = 300000


class TweetReporter(threading.Thread):
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
        if current_millis - FIVE_MINUTES_IN_MS > start_time:
            # if more than 5 mins have elapsed then start deleting tweets which are older
            # than 5 mins. This way we will maintain a window of latest tweets only (last 5 mins only)
            self.delete_old_tweets(current_millis)

        # generate reports based on last five mins data
        if len(tweet_list) > 0:
            self.preprocess_tweets(current_millis)
            self.generate_user_report()
            self.generate_content_report()
            self.generate_links_report()
        else:
            if rate_limit_reached:
                print("Rate limit has reached. Deferring report generation.")
            else:
                print("No tweets found")

    def delete_old_tweets(self, current_millis):
        """
        Deletes tweets which are older than 5 mins

        :param current_millis: timestamp when current instance of report generation started
        :return:
        """
        global tweet_list
        print("\nTweet list length before deleting old tweets {}".format(str(len(tweet_list))))
        first_min_end_ts = current_millis - 15000  # current time - 5 mins
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
        print("Number of tweets deleted {}".format(str(index + 1)))
        print("Tweet list length after deleting old tweets {}".format(str(len(tweet_list))))

    def preprocess_tweets(self, current_millis):
        """
        Preprocess tweets, within the last 5 mins, to populate global dictionaries with
        details of users, words, links which will be used for report generation

        :param current_millis: timestamp when current instance of report generation started
        :return:
        """
        global tweet_list, count_of_user_tweets, count_of_unique_words, count_of_each_domain_url, total_count_of_urls
        # clear global dictionaries to start afresh
        count_of_each_domain_url.clear()
        count_of_user_tweets.clear()
        count_of_unique_words.clear()
        total_count_of_urls = 0

        # filter tweets to fetch the latest tweets (within last 5 mins)
        tweets_in_last_5_min_window = []
        for tweet in tweet_list:
            try:
                tweet_timestamp = int(tweet['timestamp_ms'])
                if (current_millis - FIVE_MINUTES_IN_MS) <= tweet_timestamp <= current_millis \
                        and 'retweeted_status' not in tweet.keys():
                    tweets_in_last_5_min_window.append(tweet)
            except:
                print('Error while processing tweet: ' + str(tweet))

        # process relevant tweets and fill up global dictionaries
        for tweet in tweets_in_last_5_min_window:
            try:
                # process users
                user_id = tweet['user']['id']
                user_name = tweet['user']['name']
                count_user_tweets = count_of_user_tweets.get((user_id, user_name), 0) + 1
                count_of_user_tweets[(user_id, user_name)] = count_user_tweets

                # process urls
                if 'extended_tweet' in tweet.keys():
                    # text contains truncated tweet, so pick from extended_tweet
                    tweet_text = tweet['extended_tweet']['full_text']
                    list_of_urls = tweet['extended_tweet']['entities']['urls']
                else:
                    tweet_text = tweet['text']
                    list_of_urls = tweet['entities']['urls']

                if len(list_of_urls) != 0:
                    for url_in_tweet in list_of_urls:
                        total_count_of_urls += 1
                        expanded_url = url_in_tweet['expanded_url']
                        domain = re.search('\/\/([a-zA-Z0-9.-]*)[\/\?]?', expanded_url)
                        if domain:
                            domain_name = domain.group(1)
                            url_count = count_of_each_domain_url.get(domain_name, 0) + 1
                            count_of_each_domain_url[domain_name] = url_count

                # process words
                tweet_text = re.sub(r'@\w+', '', tweet_text)  # remove @ mentions
                tweet_blob = TextBlob(tweet_text.lower())  # tokenize the tweet into words without punctuations

                for word in tweet_blob.words:
                    if word not in stop_words and word != "'s" and len(word) > 1:
                        count_of_word = count_of_unique_words.get(word, 0) + 1
                        count_of_unique_words[word] = count_of_word

            except:
                print('Error in tweet: ' + str(tweet))

    def generate_user_report(self):
        global count_of_user_tweets
        if count_of_user_tweets is None or len(count_of_user_tweets) == 0:
            print('zero users have tweeted with the given keyword')
        else:
            for (user_id, user_name) in count_of_user_tweets:
                print('{user} has posted {count} tweets'.format(user=user_name,
                                                                count=count_of_user_tweets[(user_id, user_name)]))

    def generate_links_report(self):
        global count_of_each_domain_url, total_count_of_urls
        if count_of_each_domain_url is None or len(count_of_each_domain_url) == 0:
            print('zero urls till now')
        else:
            print('List of unique domains in decreasing frequency:')
            sorted_domain_with_frequency = [k for k in
                                            sorted(count_of_each_domain_url, key=count_of_each_domain_url.get,
                                                   reverse=True)]
            for domain in sorted_domain_with_frequency:
                print('{domain} :   {count}'.format(domain=domain, count=count_of_each_domain_url[domain]))

    def generate_content_report(self):
        global count_of_unique_words
        if count_of_unique_words is None or len(count_of_unique_words) == 0:
            print('zero words till now')

        else:
            print('No. of unique words till now: {count}'.format(count=len(count_of_unique_words.keys())))
            sorted_word_count = [k for k in sorted(count_of_unique_words, key=count_of_unique_words.get, reverse=True)]
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
        global start_time
        start_time = int(round(time.time() * 1000))
        while True:
            # wait for 60 seconds before generating the report
            time.sleep(5)
            # generate report in a new thread
            TweetReporter(2, "Tweet Reporter").start()


class TwitterStreamListener(StreamListener):
    """
    Custom listener which processes each tweet received via the Twitter Streaming API
    """

    def on_data(self, data):
        global tweet_list
        json_data = json.loads(data)
        tweet_list.append(json_data)
        return True

    def on_error(self, status):
        global rate_limit_reached
        if status == 420:
            rate_limit_reached = True
            print('Rate limit reached. Retrying in some time.')
        return True

    def on_connect(self):
        global rate_limit_reached
        rate_limit_reached = False
        print("Connection Established")


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
        keyword = input('Enter the keyword: ')
        if keyword == ' ' or keyword == '':
            keyword = None

    # initialize stream listener and start accepting tweets via twitter streams API
    stream_listener = TwitterStreamListener()
    stream = Stream(auth, stream_listener)
    stream.filter(track=[keyword], async=True)

    # start a new thread to generate reports periodically
    TweetTimer(1, 'Tweet Timer').start()
