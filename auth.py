import oauth2
import config
def oauth_req(url, http_method="GET", http_headers=None):
    secrets=config.my_twitter_keys()
    consumer = oauth2.Consumer(key=secrets['consumer_key'], secret=secrets['consumer_secret'])
    token = oauth2.Token(key=secrets['token_key'], secret=secrets['token_secret'])
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, method=http_method, headers=http_headers )
    return content