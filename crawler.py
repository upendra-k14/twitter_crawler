import json
import sys
import time
import tweepy
from datetime import datetime
from tweepy.error import RateLimitError, TweepError

FILTER_RETWEETS = True
MAX_ATTEMPTS = 10
DELAY = 15*60
TOKEN_PRIORITY = []

def get_tokens(file_name):
    """
    Get list of tokens
    :param file_name: name of json file containing tokens
    """

    token_list = json.load(open(file_name))
    twitter_apps_list = []
    for x in token_list:
        auth = tweepy.OAuthHandler(x[0],x[1])
        auth.set_access_token(x[2],x[3])
        api = tweepy.API(auth, retry_count=2, retry_delay=10)
        twitter_apps_list.append(api)
        TOKEN_PRIORITY.append(len(twitter_apps_list)-1)
    return twitter_apps_list

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")

def crawl_tweet(twitter_apps_list, search_term, max_tweets):
    """
    Abstracts out the crawling of tweets
    :param twitter_apps_list: List of authenticated twitter crawlers
    :param search_term: keyword to be searched
    :max_tweets: max number of tweets to be retrieved
    """

    # Initilialized all applications
    print('Initilialized all applications')

    print('Trying token 0 ...')
    api = twitter_apps_list[0]
    temp_token = TOKEN_PRIORITY.pop(0)
    TOKEN_PRIORITY.append(temp_token)

    tweet_data = []

    if(FILTER_RETWEETS):
        search_term += ' -filter:retweets'
    kwargs = {
        'q':search_term,
        'count':100,
        'lang':'en',
        'include_entities':True}

    #Do first query in order to get max_id
    #results = tweepy.Cursor(api.search, **kwargs)
    while not test_rate_limit(api, wait=False):
        print('Trying token {} ...'.format(TOKEN_PRIORITY[0]))
        api = twitter_apps_list[TOKEN_PRIORITY[0]]
        temp_token = TOKEN_PRIORITY.pop(0)
        TOKEN_PRIORITY.append(temp_token)

    test_rate_limit(api)

    try:
        search_results = api.search(**kwargs)
        for result in search_results:
            tweet_data.append((int(result.id), result.text, result.created_at))
            print(len(tweet_data))

    except RateLimitError:
        if not test_rate_limit(api, wait=False):
            print('Trying token {} ...'.format(TOKEN_PRIORITY[0]))
            api = twitter_apps_list[TOKEN_PRIORITY[0]]
            temp_token = TOKEN_PRIORITY.pop(0)
            TOKEN_PRIORITY.append(temp_token)

    except TweepError as e:
        print(e)

    #Subsequent queries use the lowest id as max_id for next query
    max_id = tweet_data[-1][0]-1
    kwargs['max_id'] = max_id
    print('FIRST QUERY OVER')
    final_data = crawl_using_max_id(twitter_apps_list, api, kwargs, tweet_data, max_tweets)

    return final_data

def crawl_using_max_id(twitter_apps_list, api, kwargs, tweet_data, max_tweets):
    """
    Abstracts out the crawling of tweets
    :param api: authenticated api object
    :param kwargs: dictionary arguments
    :tweet_data: list of tweet data
    :tweet_ids: list of tweet ids
    :max_tweets: maximum number of tweets to be collected
    """

    while(len(tweet_data)<=max_tweets):
        try:
            search_results = api.search(**kwargs)
            for result in search_results:
                tweet_data.append((int(result.id), result.text, result.created_at))
                print(len(tweet_data))
            kwargs['max_id'] = tweet_data[-1][0]-1

        except RateLimitError:
            if not test_rate_limit(api, wait=False):
                print('Trying token {} ...'.format(TOKEN_PRIORITY[0]))
                api = twitter_apps_list[TOKEN_PRIORITY[0]]
                temp_token = TOKEN_PRIORITY.pop(0)
                TOKEN_PRIORITY.append(temp_token)
            kwargs['max_id'] = tweet_data[-1][0]-1

        except TweepError as e:
            print(e)
            print("Other exception occured. Storing collected tweets ...")
            break

    return tweet_data

def test_rate_limit(api, wait=True, buffer=.1):
    """
    Tests whether the rate limit of the last request has been reached.
    :param api: The `tweepy` api instance.
    :param wait: A flag indicating whether to wait for the rate limit reset
                 if the rate limit has been reached.
    :param buffer: A buffer time in seconds that is added on to the waiting
                   time as an extra safety margin.
    :return: True if it is ok to proceed with the next request. False otherwise.
    """
    #Get the number of remaining requests
    limits = api.rate_limit_status()
    remaining = int(limits['resources']['search']['/search/tweets']['remaining'])
    #Check if we have reached the limit
    if remaining == 0:
        limit = int(limits['resources']['search']['/search/tweets']['limit'])
        reset = int(limits['resources']['search']['/search/tweets']['reset'])
        print(limit, reset)
        #Parse the UTC time
        reset = datetime.fromtimestamp(reset)
        #Let the user know we have reached the rate limit
        print("0 of {} requests remaining until {}.".format(limit, reset))

        if wait:
            #Determine the delay and sleep
            delay = (reset - datetime.now()).total_seconds() + buffer
            print("Sleeping for {}s...".format(delay))
            time.sleep(delay)
            #We have waited for the rate limit reset. OK to proceed.
            return True
        else:
            #We have reached the rate limit. The user needs to handle the rate limit manually.
            return False

    #We have not reached the rate limit
    return True

def main():
    max_tweets = 4000
    tweet_data = crawl_tweet(get_tokens(sys.argv[1]),'#CrookedHillary', max_tweets)

    export_file_name = sys.argv[2]
    json.dump(tweet_data, open(export_file_name,'w'), indent=4, default=json_serial)

if __name__ == "__main__":
    main()
