import json
import threading
import time

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from stop_words import get_stop_words
from textblob import TextBlob

stop_words = get_stop_words('english')
stop_words.append(get_stop_words('en'))
# print(get_stop_words('en'))
# print(stop_words)

count_of_user_tweets = dict()
count_of_unique_words = dict()

import config

lst =[]

class TweetProcessor(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name


    def run(self):
        while True:
            time.sleep(10)
            self.generate_reports()


    def generate_user_report(self):
        global lst
        if lst is not None and len(lst)!=0:
            global count_of_user_tweets, count_of_unique_words
            count_of_unique_words.clear()
            count_of_user_tweets.clear()
            try:
                for tweet in lst:
                    user_name = tweet['user']['name']

                    count_user_tweets = count_of_user_tweets.get(user_name,0)+1
                    count_of_user_tweets[user_name]=count_user_tweets
                    tweet_text = tweet['text']
                    # for word in tweet_text.split(' '):
                    tweet_blob = TextBlob(tweet_text.lower())
                    for word in tweet_blob.words:
                        if word not in stop_words and word!= "'s" and len(word)>1:
                            count_of_word = count_of_unique_words.get(word, 0) + 1
                            count_of_unique_words[word] = count_of_word

            except:
                print(tweet)


        if count_of_user_tweets is None or len(count_of_user_tweets)==0:
            print('zero users have tweeted')
        else:
            for user in count_of_user_tweets:
                print('{user} has posted {count} tweets'.format(user=user,count=count_of_user_tweets[user]))

    def generate_links_report(self):
        pass

    def generate_content_report(self):
        global count_of_unique_words
        if count_of_unique_words is None or len(count_of_unique_words)==0:
            print('zero words till now')

        else:
            # for word in count_of_unique_words:
            #     print('{word}:{count}'.format(word=word,count=count_of_unique_words[word]))
            print('\nNo. of unique words till now: {count}'.format(count=len(count_of_unique_words.keys())))
            sorted_word_count = [k for k in sorted(count_of_unique_words, key=count_of_unique_words.get, reverse=True)]
            print('\nThe most frequently used 10 words are:')
            for i in range(0,10):
                print(sorted_word_count[i])


    def generate_reports(self):
        self.generate_user_report()
        self.generate_content_report()
        self.generate_links_report()




def timer():
   now = time.localtime(time.time())
   return now[5]

class StdOutListener(StreamListener):
    def on_data(self, data):
        global lst
        json_data = json.loads(data)
        lst.append(json_data)
        return True

    def on_error(self, status):
        if status==420:
            print('rate limit reached')
            return False


if __name__=="__main__":
    l = StdOutListener()
    secrets = config.my_twitter_keys()
    consumer_key = secrets['consumer_key']
    consumer_secret = secrets['consumer_secret']
    token_key = secrets['token_key']
    token_secret = secrets['token_secret']
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(token_key,token_secret)
    keyword = None
    while keyword is None:
        keyword = input('enter the keyword: ')
        if keyword==' ' or keyword=='':
            keyword = None

    stream = Stream(auth,l)

    stream.filter(track=[keyword],async=True)
    thread = TweetProcessor(1, 'ReportThread')
    thread.start()





