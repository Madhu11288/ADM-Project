import time

__author__ = 'tvsamartha'

import aerospike
from aerospike import predicates as p


AEROSPIKE_NS = "test"
AEROSPIKE_STORMSET = "stormset";
TWEET_ID_BIN = "tweetId_bin";
TWEET_DATETIME_BIN = "tweetDateTime";
TWEET_LOCATION_BIN = "tweetLocation";
TWEET_TEXT_BIN = "tweetText";
USER_NAME_BIN = "userName";
TWEET_HASHTAG_BIN = "hashTag";

config = {
    "hosts": [
        ( '127.0.0.1', 3000 )
    ],
    "policies": {
        "timeout": 1000 # milliseconds
    }
}

client = aerospike.client(config).connect()

# callback function will print the records as they are read
def print_result((key, metadata, record)):
    print(str(record['userName']) + " - " + str(record['tweetText']) + " -- " + str(record['tweetDateTime']))


def get_tweets_of_user(username):
    print("get_tweets_of_user = " + username)
    # Key of the record
    query = client.query(AEROSPIKE_NS, AEROSPIKE_STORMSET)
    query.select(USER_NAME_BIN, TWEET_TEXT_BIN, TWEET_DATETIME_BIN)
    query.where(p.equals(USER_NAME_BIN, username))

    # Execute the query and call print_result for each result
    query.foreach(print_result)

    print("----------------------------------------------")
    return

def get_tweets_in_timeframe(start_time, end_time):
    print("get_tweets_in_timeframe from " + str(start_time) + " to " + str(end_time))
    query = client.query(AEROSPIKE_NS, AEROSPIKE_STORMSET)
    query.select(USER_NAME_BIN, TWEET_TEXT_BIN, TWEET_DATETIME_BIN)
    query.where(p.between(TWEET_DATETIME_BIN, start_time, end_time))

    query.foreach(print_result)

    print("----------------------------------------------")
    return


def get_tweets_in_timeframe(start_time):
    print("get_tweets_in_timeframe from " + str(start_time) + " to current time" )
    current_time_in_millis = int(round(time.time() * 1000))

    query = client.query(AEROSPIKE_NS, AEROSPIKE_STORMSET)
    query.select(USER_NAME_BIN, TWEET_TEXT_BIN, TWEET_DATETIME_BIN)
    query.where(p.between(TWEET_DATETIME_BIN, start_time, current_time_in_millis))

    query.foreach(print_result)

    print("----------------------------------------------")
    return


#main function
get_tweets_of_user("KianJc")

mins = 1
time_in_millis = mins * 60 * 1000
get_tweets_in_timeframe(time_in_millis, time_in_millis*5)


